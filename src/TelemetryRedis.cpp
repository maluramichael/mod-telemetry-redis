/*
 * mod-telemetry-redis - worker thread, ring buffer and hiredis writer.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2, or (at your
 * option) any later version.
 *
 * FAILURE MODEL (Redis down): the world thread only ever touches the bounded
 * in-memory ring buffer, never a socket. The background worker owns the single
 * hiredis context and lazily (re)connects with exponential backoff. When there
 * is no connection, pulled batches are dropped. No hiredis call can propagate
 * an exception or a crash into the world loop.
 */

#include "TelemetryRedis.h"
#include "TelemetryApi.h"

#include "Config.h"
#include "GameTime.h"
#include "Log.h"

#include <algorithm>
#include <chrono>

#ifdef DAD_TELEMETRY_AVAILABLE
#include <hiredis/hiredis.h>
#endif

namespace
{
    // Hard caps to keep the worker responsive and bound per-iteration work.
    constexpr size_t kMaxBatch = 512;
    constexpr uint32 kBackoffMinMs = 1000;
    constexpr uint32 kBackoffMaxMs = 30000;
}

namespace DadTelemetry
{
    Telemetry& Telemetry::instance()
    {
        static Telemetry inst;
        return inst;
    }

    Telemetry::~Telemetry()
    {
        Stop();
    }

    void Telemetry::LoadConfig()
    {
        bool const dadMode = sConfigMgr->GetOption<bool>("DadMode.Enabled", false);
        bool const telemetry = sConfigMgr->GetOption<bool>("DadMode.Telemetry.Enabled", false);

        _enabledCfg = dadMode && telemetry;
        _host = sConfigMgr->GetOption<std::string>("DadMode.Telemetry.RedisHost", "127.0.0.1");
        _port = sConfigMgr->GetOption<int32>("DadMode.Telemetry.RedisPort", 6379);
        _stream = sConfigMgr->GetOption<std::string>("DadMode.Telemetry.Stream", "wow:events");
        _positionIntervalMs = sConfigMgr->GetOption<uint32>("DadMode.Telemetry.PositionIntervalMs", 5000);
        _heartbeatIntervalMs = sConfigMgr->GetOption<uint32>("DadMode.Telemetry.HeartbeatIntervalMs", 5000);

        int64 qmax = sConfigMgr->GetOption<int64>("DadMode.Telemetry.QueueMax", 100000);
        if (qmax < 100)
            qmax = 100;
        _queueMax = static_cast<size_t>(qmax);

        if (_positionIntervalMs < 250)
            _positionIntervalMs = 250;

        if (_heartbeatIntervalMs < 1000)
            _heartbeatIntervalMs = 1000;

#ifndef DAD_TELEMETRY_AVAILABLE
        if (_enabledCfg)
            LOG_WARN("module", "[mod-telemetry-redis] enabled in config but built WITHOUT hiredis - telemetry disabled.");
        _enabledCfg = false;
#endif
    }

    void Telemetry::Start()
    {
        if (!_enabledCfg)
        {
            _active.store(false, std::memory_order_relaxed);
            return;
        }

        if (_running.load(std::memory_order_acquire))
            return; // already running

#ifdef DAD_TELEMETRY_AVAILABLE
        _active.store(true, std::memory_order_relaxed);
        _running.store(true, std::memory_order_release);
        try
        {
            _worker = std::thread(&Telemetry::WorkerMain, this);
        }
        catch (std::exception const& e)
        {
            _running.store(false, std::memory_order_release);
            _active.store(false, std::memory_order_relaxed);
            LOG_ERROR("module", "[mod-telemetry-redis] failed to start worker thread: {}", e.what());
            return;
        }
        LOG_INFO("module", "[mod-telemetry-redis] telemetry active -> redis {}:{} stream '{}' (interval {} ms, queue max {}).",
                 _host, _port, _stream, _positionIntervalMs, _queueMax);
#else
        _active.store(false, std::memory_order_relaxed);
#endif
    }

    void Telemetry::Stop()
    {
        if (!_running.exchange(false, std::memory_order_acq_rel))
        {
            _active.store(false, std::memory_order_relaxed);
            return;
        }

        _active.store(false, std::memory_order_relaxed);
        _cv.notify_all();

        if (_worker.joinable())
        {
            try
            {
                _worker.join();
            }
            catch (...)
            {
                // detach as a last resort so shutdown never hangs/crashes
                try { _worker.detach(); } catch (...) {}
            }
        }
    }

    void Telemetry::Push(std::string const& type, std::vector<std::pair<std::string, std::string>> const& fields)
    {
        if (!_active.load(std::memory_order_relaxed))
            return;

        Event ev;
        ev.type = type;
        ev.fields.reserve(fields.size() + 2);

        // Auto-added fields captured on the calling (world) thread.
        // ts is UNIX SECONDS (GetGameTime, not GetGameTimeMS) so it matches the
        // unix-seconds ts used by mod-bot-economy and the manager consumer's
        // freshness/online window — a ms-since-start value made every row look
        // stale (bots.online=0) and could freeze positions via GREATEST(ts).
        ev.fields.emplace_back("ts", std::to_string(static_cast<uint64>(GameTime::GetGameTime().count())));
        ev.fields.emplace_back("fork_sha", DAD_FORK_SHA);
        for (auto const& f : fields)
            ev.fields.push_back(f);

        {
            std::lock_guard<std::mutex> lk(_mutex);
            if (_queue.size() >= _queueMax)
            {
                // Drop oldest to bound memory; log sparsely.
                _queue.pop_front();
                if ((++_dropped % 10000) == 1)
                    LOG_WARN("module", "[mod-telemetry-redis] queue full ({}), dropping oldest events (total dropped {}).",
                             _queueMax, _dropped);
            }
            _queue.push_back(std::move(ev));
        }
        _cv.notify_one();
    }

#ifdef DAD_TELEMETRY_AVAILABLE
    void Telemetry::WorkerMain()
    {
        redisContext* ctx = nullptr;
        uint32 backoffMs = kBackoffMinMs;
        auto nextConnectAt = std::chrono::steady_clock::now();

        auto disconnect = [&]()
        {
            if (ctx)
            {
                redisFree(ctx);
                ctx = nullptr;
            }
        };

        auto ensureConnected = [&]() -> bool
        {
            if (ctx)
                return true;

            auto now = std::chrono::steady_clock::now();
            if (now < nextConnectAt)
                return false; // still backing off

            timeval tv;
            tv.tv_sec = 2;
            tv.tv_usec = 0;
            redisContext* c = redisConnectWithTimeout(_host.c_str(), _port, tv);
            if (!c || c->err)
            {
                if (c)
                    redisFree(c);
                nextConnectAt = now + std::chrono::milliseconds(backoffMs);
                backoffMs = std::min<uint32>(backoffMs * 2, kBackoffMaxMs);
                return false;
            }

            // 1s command timeout so a stalled Redis cannot wedge the worker.
            timeval cmdTv;
            cmdTv.tv_sec = 1;
            cmdTv.tv_usec = 0;
            redisSetTimeout(c, cmdTv);

            ctx = c;
            backoffMs = kBackoffMinMs;
            LOG_INFO("module", "[mod-telemetry-redis] connected to redis {}:{}.", _host, _port);
            return true;
        };

        auto writeEvent = [&](Event const& ev) -> bool
        {
            // XADD <stream> MAXLEN ~ <queuemax> * type <type> <field> <value> ...
            std::vector<char const*> argv;
            std::vector<size_t> lens;
            auto add = [&](std::string const& s)
            {
                argv.push_back(s.c_str());
                lens.push_back(s.size());
            };

            std::string const maxlen = std::to_string(_queueMax);
            static std::string const cmdXadd = "XADD";
            static std::string const optMaxlen = "MAXLEN";
            static std::string const optApprox = "~";
            static std::string const optStar = "*";
            static std::string const fldType = "type";

            add(cmdXadd);
            add(_stream);
            add(optMaxlen);
            add(optApprox);
            add(maxlen);
            add(optStar);
            add(fldType);
            add(ev.type);
            for (auto const& f : ev.fields)
            {
                add(f.first);
                add(f.second);
            }

            redisReply* reply = static_cast<redisReply*>(
                redisCommandArgv(ctx, static_cast<int>(argv.size()), argv.data(), lens.data()));

            if (!reply || ctx->err)
            {
                if (reply)
                    freeReplyObject(reply);
                return false;
            }
            freeReplyObject(reply);
            return true;
        };

        while (_running.load(std::memory_order_acquire))
        {
            std::vector<Event> batch;
            {
                std::unique_lock<std::mutex> lk(_mutex);
                _cv.wait_for(lk, std::chrono::milliseconds(500), [&]
                {
                    return !_running.load(std::memory_order_acquire) || !_queue.empty();
                });

                while (!_queue.empty() && batch.size() < kMaxBatch)
                {
                    batch.push_back(std::move(_queue.front()));
                    _queue.pop_front();
                }
            }

            if (batch.empty())
                continue;

            if (!ensureConnected())
                continue; // Redis down -> drop this batch, keep going

            for (auto const& ev : batch)
            {
                if (!writeEvent(ev))
                {
                    disconnect();
                    // Remaining events in this batch are dropped by design.
                    nextConnectAt = std::chrono::steady_clock::now() + std::chrono::milliseconds(backoffMs);
                    backoffMs = std::min<uint32>(backoffMs * 2, kBackoffMaxMs);
                    break;
                }
            }
        }

        // Best-effort flush on shutdown (only if we can connect quickly).
        {
            std::vector<Event> remaining;
            {
                std::lock_guard<std::mutex> lk(_mutex);
                remaining.assign(std::make_move_iterator(_queue.begin()),
                                 std::make_move_iterator(_queue.end()));
                _queue.clear();
            }
            if (!remaining.empty() && ensureConnected())
            {
                for (auto const& ev : remaining)
                {
                    if (!writeEvent(ev))
                        break;
                }
            }
        }

        disconnect();
    }
#else
    void Telemetry::WorkerMain()
    {
        // No hiredis compiled in: worker never starts.
    }
#endif

    // ---- Public API (TelemetryApi.h) ----
    void Emit(std::string const& type, std::vector<std::pair<std::string, std::string>> const& fields)
    {
        Telemetry::instance().Push(type, fields);
    }

    bool Enabled()
    {
        return Telemetry::instance().IsActive();
    }
}
