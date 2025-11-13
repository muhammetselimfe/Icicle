-- === Common role/grants ===
CREATE ROLE IF NOT EXISTS frontend_reader;
GRANT SELECT ON default.* TO frontend_reader;
GRANT SHOW ON *.* TO frontend_reader;
GRANT SELECT ON system.parts TO frontend_reader;
GRANT SELECT ON system.tables TO frontend_reader;
GRANT SELECT ON system.columns TO frontend_reader;
GRANT SELECT ON system.databases TO frontend_reader;
-- === Profiles ===

-- 1) Anonymous: fast aggs, single-threaded, small outputs, anti-full-scan tripwires
DROP SETTINGS PROFILE IF EXISTS anonymous_profile;
CREATE SETTINGS PROFILE anonymous_profile SETTINGS
    readonly = 1,
    allow_ddl = 0,
    allow_introspection_functions = 0,

    -- allow parallel queries for dashboard widgets
    max_concurrent_queries_for_user = 10,
    max_threads = 1,

    -- cap what the browser digests
    max_result_rows  = 1_000,
    max_result_bytes = 64_000_000,
    result_overflow_mode = 'break',

    -- tripwires against “scan the world”
    max_rows_to_read  = 1_000_000,        -- tune up/down after observing
    max_bytes_to_read = 1_000_000_000,     -- ~1 GB raw read ceiling

    -- let CPU rip but don’t OOM the box
    max_execution_time = 3,
    max_memory_usage   = 1_000_000_000;    -- 1 GB

-- 2) Anonymous-heavy: permit scans, but keep concurrency tiny
DROP SETTINGS PROFILE IF EXISTS anonymous_heavy_profile;
CREATE SETTINGS PROFILE anonymous_heavy_profile SETTINGS
    readonly = 1,
    allow_ddl = 0,
    allow_introspection_functions = 0,

    -- don’t let them pile up
    max_concurrent_queries_for_user = 2,

    -- no per-query read limits (this is the point)
    max_rows_to_read  = 0,
    max_bytes_to_read = 0,
    max_partitions_to_read = 0,

    -- reasonable output cap so browsers don’t choke anyway
    max_result_rows  = 1_000,
    max_result_bytes = 64_000_000,
    result_overflow_mode = 'break',

    max_execution_time = 60,
    max_memory_usage   = 0;  -- no limit

-- === Quotas (keyed by IP) ===

-- Small/agg user: lots of tiny queries allowed
DROP QUOTA IF EXISTS anonymous_quota;
CREATE QUOTA anonymous_quota KEYED BY ip_address
    FOR INTERVAL 1 minute
        MAX QUERIES 4000,
        MAX ERRORS 200;

-- Heavy user: throttle by request count; this is your "2 per minute" gate
DROP QUOTA IF EXISTS anonymous_heavy_quota;
CREATE QUOTA anonymous_heavy_quota KEYED BY ip_address
    FOR INTERVAL 1 minute
        MAX QUERIES 10,
        MAX ERRORS 10;

-- === Users (demo: open to all IPs, throttled by quotas/profiles) ===

CREATE USER IF NOT EXISTS anonymous
    IDENTIFIED WITH no_password
    HOST ANY
    DEFAULT ROLE frontend_reader
    SETTINGS PROFILE 'anonymous_profile'
    DEFAULT DATABASE default;

CREATE USER IF NOT EXISTS anonymous_heavy
    IDENTIFIED WITH no_password
    HOST ANY
    DEFAULT ROLE frontend_reader
    SETTINGS PROFILE 'anonymous_heavy_profile'
    DEFAULT DATABASE default;

-- Assign quotas to users
ALTER QUOTA anonymous_quota TO anonymous;
ALTER QUOTA anonymous_heavy_quota TO anonymous_heavy;
