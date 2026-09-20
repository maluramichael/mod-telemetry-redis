/*
 * mod-telemetry-redis - streams bot telemetry to Redis via hiredis.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2, or (at your
 * option) any later version.
 *
 * Bootability contract: nothing in this header or its implementation may crash
 * the world if Redis is unavailable. All hiredis usage lives behind the
 * DAD_TELEMETRY_AVAILABLE compile definition (set by the module cmake only when
 * hiredis is found) and is guarded/reconnected lazily; events are dropped when
 * there is no connection.
 */

#ifndef MOD_TELEMETRY_REDIS_H
#define MOD_TELEMETRY_REDIS_H

#include "Define.h"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// Fork SHA is injected by the module cmake; provide a safe fallback so the code
// compiles even when the definition is missing (e.g. hiredis-less builds).
#ifndef DAD_FORK_SHA
#define DAD_FORK_SHA "unknown"
#endif

namespace DadTelemetry
{
    // A single telemetry event captured on the world thread and drained by the
    // background worker. Fields already contain the auto-added ts / fork_sha.
    struct Event
    {
        std::string type;
        std::vector<std::pair<std::string, std::string>> fields;
    };

    // Singleton manager: bounded ring buffer + one background worker thread that
    // writes to Redis (XADD). Thread-safe. Never throws to the caller.
    class Telemetry
    {
    public:
        static Telemetry& instance();

        // Read config from sConfigMgr. Safe to call multiple times
        // (OnAfterConfigLoad). Does not start/stop the worker by itself.
        void LoadConfig();

        // Start the worker thread if telemetry is active and not already running.
        void Start();

        // Stop the worker thread, best-effort flush, join. Safe to call twice.
        void Stop();

        // True when DadMode.Enabled && DadMode.Telemetry.Enabled and hiredis is
        // compiled in. When false, Emit is a cheap no-op.
        bool IsActive() const { return _active.load(std::memory_order_relaxed); }

        // Push an event onto the ring buffer (drops oldest when full). Auto-adds
        // ts (unix seconds, GameTime::GetGameTime) and fork_sha. Non-blocking,
        // never throws.
        void Push(std::string const& type, std::vector<std::pair<std::string, std::string>> const& fields);

        // Config accessors used by the script hooks.
        uint32 PositionIntervalMs() const { return _positionIntervalMs; }
        uint32 HeartbeatIntervalMs() const { return _heartbeatIntervalMs; }

    private:
        Telemetry() = default;
        ~Telemetry();
        Telemetry(Telemetry const&) = delete;
        Telemetry& operator=(Telemetry const&) = delete;

        void WorkerMain();

        // --- config ---
        bool _enabledCfg = false;         // DadMode.Enabled && DadMode.Telemetry.Enabled
        std::string _host = "127.0.0.1";
        int _port = 6379;
        std::string _stream = "wow:events";
        uint32 _positionIntervalMs = 5000;
        uint32 _heartbeatIntervalMs = 5000;
        size_t _queueMax = 100000;

        // --- runtime ---
        std::atomic<bool> _active{false};   // telemetry on AND worker should run
        std::atomic<bool> _running{false};  // worker loop alive
        std::thread _worker;

        std::mutex _mutex;
        std::condition_variable _cv;
        std::deque<Event> _queue;
        uint64 _dropped = 0;                // count of dropped events (buffer overflow)
    };
}

#endif // MOD_TELEMETRY_REDIS_H
