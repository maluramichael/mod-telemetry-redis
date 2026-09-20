/*
 * mod-telemetry-redis - WorldScript / PlayerScript hooks.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2, or (at your
 * option) any later version.
 *
 * All hooks only build small field vectors and hand them to DadTelemetry::Emit
 * (a non-blocking ring-buffer push). No socket / hiredis work happens on the
 * world thread, so a dead Redis cannot stall or crash the world.
 */

#include "TelemetryApi.h"
#include "TelemetryRedis.h"

#include "Config.h"
#include "Creature.h"
#include "GameTime.h"
#include "Item.h"
#include "ObjectAccessor.h"
#include "ObjectMgr.h"
#include "Player.h"
#include "ScriptMgr.h"
#include "UpdateFields.h"
#include "UpdateTime.h"

// Playerbots fork: bot detection. Use PlayerbotsMgr::instance() (dot, not ->),
// matching the fork-compatible usage in the other bot modules.
#include "PlayerbotMgr.h"

#include <algorithm>
#include <string>
#include <vector>

namespace
{
    using Field = std::pair<std::string, std::string>;
    using Fields = std::vector<Field>;

    // Cap on bot_position emits per position tick, so 500 bots never flood.
    constexpr uint32 kPositionSampleCap = 50;

    inline bool IsBot(Player* player)
    {
        return player && PlayerbotsMgr::instance().GetPlayerbotAI(player) != nullptr;
    }

    // Fill common identity/position context for a bot player.
    void AppendBotContext(Fields& f, Player* p)
    {
        f.emplace_back("bot", std::to_string(p->GetGUID().GetCounter()));
        f.emplace_back("name", p->GetName());
        f.emplace_back("map", std::to_string(p->GetMapId()));
        f.emplace_back("zone", std::to_string(p->GetZoneId()));
        f.emplace_back("area", std::to_string(p->GetAreaId()));
        f.emplace_back("x", std::to_string(p->GetPositionX()));
        f.emplace_back("y", std::to_string(p->GetPositionY()));
        f.emplace_back("z", std::to_string(p->GetPositionZ()));
        f.emplace_back("level", std::to_string(p->GetLevel()));
        f.emplace_back("race", std::to_string(p->getRace()));
        f.emplace_back("cls", std::to_string(p->getClass()));
        f.emplace_back("faction", (p->GetTeamId() == TEAM_ALLIANCE) ? "0" : "1");
    }
}

class TelemetryWorldScript : public WorldScript
{
public:
    TelemetryWorldScript() : WorldScript("TelemetryWorldScript") {}

    void OnAfterConfigLoad(bool /*reload*/) override
    {
        DadTelemetry::Telemetry::instance().LoadConfig();
    }

    void OnStartup() override
    {
        // Config already loaded via OnAfterConfigLoad; (re)read and start worker.
        DadTelemetry::Telemetry::instance().LoadConfig();
        DadTelemetry::Telemetry::instance().Start();
    }

    void OnShutdown() override
    {
        DadTelemetry::Telemetry::instance().Stop();
    }

    void OnUpdate(uint32 diff) override
    {
        if (!DadTelemetry::Enabled())
            return;

        // World heartbeat: emitted on its own timer, independent of the position
        // sampler, so the manager's Systems view has real TPS / map diff / uptime.
        EmitHeartbeat(diff);

        uint32 const interval = DadTelemetry::Telemetry::instance().PositionIntervalMs();
        _accum += diff;
        if (_accum < interval)
            return;
        _accum = 0;

        // Collect currently-online bots, then emit a rotating capped subset so
        // over successive ticks all bots are covered without flooding.
        std::vector<Player*> bots;
        auto const& players = ObjectAccessor::GetPlayers();
        bots.reserve(players.size());
        for (auto const& kv : players)
        {
            Player* p = kv.second;
            if (p && p->IsInWorld() && IsBot(p))
                bots.push_back(p);
        }

        if (bots.empty())
        {
            _posCursor = 0;
            return;
        }

        if (_posCursor >= bots.size())
            _posCursor = 0;

        uint32 emitted = 0;
        size_t idx = _posCursor;
        size_t const total = bots.size();
        for (size_t scanned = 0; scanned < total && emitted < kPositionSampleCap; ++scanned)
        {
            Player* p = bots[idx];
            Fields f;
            f.reserve(12);
            AppendBotContext(f, p);
            DadTelemetry::Emit("bot_position", f);
            ++emitted;
            idx = (idx + 1) % total;
        }
        _posCursor = idx;
    }

private:
    // Periodic world heartbeat carrying tps / map_diff_ms / uptime_sec / bots_online.
    void EmitHeartbeat(uint32 diff)
    {
        uint32 const interval = DadTelemetry::Telemetry::instance().HeartbeatIntervalMs();
        _hbAccum += diff;
        if (_hbAccum < interval)
            return;
        _hbAccum = 0;

        // Count currently-online playerbots (same IsBot loop as position sampling).
        uint32 botsOnline = 0;
        auto const& players = ObjectAccessor::GetPlayers();
        for (auto const& kv : players)
        {
            Player* p = kv.second;
            if (p && p->IsInWorld() && IsBot(p))
                ++botsOnline;
        }

        // Average world update time (ms) over the recent window; used directly as
        // map_diff_ms. tps is an approximation derived from it (bounded).
        uint32 const avgUpdateMs = sWorldUpdateTime.GetAverageUpdateTime();
        double const tps = 1000.0 / static_cast<double>(std::max<uint32>(1u, avgUpdateMs));
        uint64 const uptimeSec = static_cast<uint64>(GameTime::GetUptime().count());

        Fields f;
        f.reserve(4);
        f.emplace_back("tps", std::to_string(tps)); // approximate: 1000 / avg update ms
        f.emplace_back("map_diff_ms", std::to_string(avgUpdateMs));
        f.emplace_back("uptime_sec", std::to_string(uptimeSec));
        f.emplace_back("bots_online", std::to_string(botsOnline));
        DadTelemetry::Emit("heartbeat", f);
    }

    uint32 _accum = 0;
    uint32 _hbAccum = 0;
    size_t _posCursor = 0;
};

class TelemetryPlayerScript : public PlayerScript
{
public:
    // Fork uses an OPT-IN hook mask: pass every PLAYERHOOK_* this script
    // overrides to the base ctor. (An empty list would enable ALL hooks, which
    // is what this script relied on before; the explicit list is equivalent for
    // the hooks below and must list all of them or those hooks stop firing.)
    TelemetryPlayerScript()
        : PlayerScript("TelemetryPlayerScript",
                       {PLAYERHOOK_ON_LOGIN,
                        PLAYERHOOK_ON_LOOT_ITEM,
                        PLAYERHOOK_ON_GIVE_EXP,
                        PLAYERHOOK_ON_CREATURE_KILL,
                        PLAYERHOOK_ON_PLAYER_QUEST_ACCEPT,
                        PLAYERHOOK_ON_PLAYER_COMPLETE_QUEST,
                        PLAYERHOOK_ON_QUEST_ABANDON})
    {
    }

    void OnPlayerLogin(Player* player) override
    {
        if (!DadTelemetry::Enabled() || !IsBot(player))
            return;

        Fields f;
        f.reserve(12);
        AppendBotContext(f, player);
        DadTelemetry::Emit("bot_login", f);
    }

    void OnPlayerLootItem(Player* player, Item* item, uint32 count, ObjectGuid /*lootguid*/) override
    {
        if (!DadTelemetry::Enabled() || !IsBot(player) || !item)
            return;

        Fields f;
        f.emplace_back("bot", std::to_string(player->GetGUID().GetCounter()));
        f.emplace_back("name", player->GetName());
        f.emplace_back("item", std::to_string(item->GetEntry()));
        f.emplace_back("count", std::to_string(count));
        f.emplace_back("map", std::to_string(player->GetMapId()));
        f.emplace_back("zone", std::to_string(player->GetZoneId()));
        f.emplace_back("level", std::to_string(player->GetLevel()));
        DadTelemetry::Emit("bot_loot", f);
    }

    void OnPlayerGiveXP(Player* player, uint32& amount, Unit* /*victim*/, uint8 /*xpSource*/) override
    {
        if (!DadTelemetry::Enabled() || !IsBot(player))
            return;

        // Approximate level-up: emit only when this XP grant crosses the
        // next-level boundary. Fires before the level is actually applied.
        uint32 const nextLevelXp = player->GetUInt32Value(PLAYER_NEXT_LEVEL_XP);
        if (nextLevelXp == 0)
            return; // at level cap (no further XP curve)

        uint32 const curXp = player->GetUInt32Value(PLAYER_XP);
        if (static_cast<uint64>(curXp) + amount < nextLevelXp)
            return; // no boundary crossed this grant

        Fields f;
        f.emplace_back("bot", std::to_string(player->GetGUID().GetCounter()));
        f.emplace_back("name", player->GetName());
        f.emplace_back("from_level", std::to_string(player->GetLevel()));
        f.emplace_back("to_level", std::to_string(player->GetLevel() + 1)); // approximate
        f.emplace_back("map", std::to_string(player->GetMapId()));
        f.emplace_back("zone", std::to_string(player->GetZoneId()));
        DadTelemetry::Emit("bot_levelup", f);
    }

    void OnPlayerCreatureKill(Player* killer, Creature* killed) override
    {
        if (!DadTelemetry::Enabled() || !IsBot(killer))
            return;

        // Lightweight position/kill context only. Gold-for-kills is emitted by
        // mod-bot-economy; we deliberately do NOT emit gold here (no duplication).
        Fields f;
        f.emplace_back("bot", std::to_string(killer->GetGUID().GetCounter()));
        f.emplace_back("name", killer->GetName());
        f.emplace_back("victim", killed ? std::to_string(killed->GetEntry()) : "0");
        f.emplace_back("map", std::to_string(killer->GetMapId()));
        f.emplace_back("zone", std::to_string(killer->GetZoneId()));
        f.emplace_back("area", std::to_string(killer->GetAreaId()));
        f.emplace_back("x", std::to_string(killer->GetPositionX()));
        f.emplace_back("y", std::to_string(killer->GetPositionY()));
        f.emplace_back("z", std::to_string(killer->GetPositionZ()));
        f.emplace_back("level", std::to_string(killer->GetLevel()));
        DadTelemetry::Emit("bot_kill", f);
    }

    // Quest telemetry now emitted from CORE hooks so it fires on ALL bot quest
    // activity during normal leveling (not just the rarely-used bot actions),
    // and carries the NUMERIC quest id (the consumer casts `quest` to int).
    void OnPlayerQuestAccept(Player* player, Quest const* quest) override
    {
        if (!DadTelemetry::Enabled() || !IsBot(player) || !quest)
            return;

        Fields f;
        f.emplace_back("bot", std::to_string(player->GetGUID().GetCounter()));
        f.emplace_back("name", player->GetName());
        f.emplace_back("quest", std::to_string(quest->GetQuestId()));
        f.emplace_back("title", quest->GetTitle());
        f.emplace_back("state", "accept");
        f.emplace_back("reason", "accepted");
        DadTelemetry::Emit("bot_quest", f);
    }

    // Fires at the end of RewardQuest = the real "quest finished" moment.
    void OnPlayerCompleteQuest(Player* player, Quest const* quest) override
    {
        if (!DadTelemetry::Enabled() || !IsBot(player) || !quest)
            return;

        Fields f;
        f.emplace_back("bot", std::to_string(player->GetGUID().GetCounter()));
        f.emplace_back("name", player->GetName());
        f.emplace_back("quest", std::to_string(quest->GetQuestId()));
        f.emplace_back("title", quest->GetTitle());
        f.emplace_back("state", "complete");
        f.emplace_back("reason", "turned in");
        DadTelemetry::Emit("bot_quest", f);
    }

    void OnPlayerQuestAbandon(Player* player, uint32 questId) override
    {
        if (!DadTelemetry::Enabled() || !IsBot(player))
            return;

        Quest const* quest = sObjectMgr->GetQuestTemplate(questId);
        Fields f;
        f.emplace_back("bot", std::to_string(player->GetGUID().GetCounter()));
        f.emplace_back("name", player->GetName());
        f.emplace_back("quest", std::to_string(questId));
        f.emplace_back("title", quest ? quest->GetTitle() : "");
        f.emplace_back("state", "abandon");
        f.emplace_back("reason", "abandoned");
        DadTelemetry::Emit("bot_quest", f);
    }
};

// Entry point invoked by the module loader (src/loader.cpp).
void AddTelemetryRedisScripts()
{
    new TelemetryWorldScript();
    new TelemetryPlayerScript();
}
