create database RealtimeEcommerceDB;
go

use RealtimeEcommerceDB
go

create table events (
    event_id       varchar(100) PRIMARY KEY,
    event_type     varchar(50) NOT NULL,
    order_id       varchar(50),
    customer_id    varchar(50),
    amount         decimal(12, 2),
    event_time     datetime2 NOT NULL,
    ingestion_time datetime2 NOT NULL,
    source_offset  bigint NOT NULL,
    batch_id       varchar(50),
    is_late        bit default 0
);

create table event_quarantine (
    quarantine_id bigint identity(1,1) PRIMARY KEY,
    event_id      varchar(100),
    raw_event     nvarchar(MAX),
    error_reason  nvarchar(500),
    quarantined_at datetime2 default SYSUTCDATETIME()
);


create table pipeline_checkpoints (
    source_name      varchar(100) PRIMARY KEY,
    last_offset      bigint NOT NULL default 0,
    updated_at       datetime2 default SYSUTCDATETIME()
);

create table pipeline_runs (
    run_id          varchar(50) primary key,
    batch_id        varchar(50),
    started_at      datetime2,
    completed_at    datetime2,
    status          varchar(20),
    records_read    int default 0,
    records_inserted int default 0,
    records_rejected int default 0,
    error_message   nvarchar(max)
);

create table minute_aggregates (
    window_start       datetime2 NOT NULL,
    event_type         varchar(50) NOT NULL,
    event_count        int NOT NULL,
    total_amount       decimal(14, 2) NOT NULL default 0,
    updated_at         datetime2 default SYSUTCDATETIME(),
    CONSTRAINT PK_minute_aggregates
        PRIMARY KEY (window_start, event_type)
);

CREATE TABLE dbo.processed_events (
    event_id VARCHAR(100) NOT NULL,
    source_offset BIGINT NOT NULL,
    processed_at DATETIME2 NOT NULL
        CONSTRAINT DF_processed_events_processed_at
        DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_processed_events
        PRIMARY KEY (event_id),

    CONSTRAINT UQ_processed_events_source_offset
        UNIQUE (source_offset)
);
GO

select TABLE_NAME
from INFORMATION_SCHEMA.TABLES
where TABLE_TYPE = 'BASE TABLE';


select count(*) as TotalEvents from events;
select * from events;

select top 10 *
from events
order by source_offset;

select
    count(*) as TotalEvents,
    count(distinct event_id) as UniqueEvents,
    count(distinct source_offset) as UniqueOffsets
from events;

select
    event_type,
    count(*) as TotalEvents,
    sum(amount) as TotalAmount
from events
group by event_type;


-- Check the checkpoint
select *
from pipeline_checkpoints;


-- Check the minute-wise aggregates
select
    window_start,
    event_type,
    event_count,
    total_amount
from dbo.minute_aggregates
order by window_start, event_type;

-- Check total aggregated events
select sum(event_count) as TotalProcessedEvents
from dbo.minute_aggregates;

select sum(event_count) as TotalProcessedEvents
from dbo.minute_aggregates;

select top 10 event_id,event_type,event_time,ingestion_time,
    DATEDIFF(
        MINUTE,event_time,ingestion_time
    ) as DelayMinutes
from events
order by DelayMinutes desc;

--Check your existing late-event count
select
    count(*) AS TotalEvents,
    sum(case
        when DATEDIFF(MINUTE, event_time, ingestion_time) > 5
        then 1 ELSE 0
    end) as LateEvents
from events;


-- Mark late events in SQL Server
update events
set is_late =
    case
        when DATEDIFF(
            MINUTE,
            event_time,
            ingestion_time
        ) > 5
        then 1
        else 0
    end;


select
    count(*) AS TotalEvents,
    sum(cast(is_late as int)) as LateEvents,
    sum(case when is_late = 0 then 1 else 0 end) as OnTimeEvents
from events;

--Inspect your minute aggregates
select event_type,sum(event_count) as TotalEvents,
    sum(total_amount) as TotalAmount
from minute_aggregates
group by event_type
order by event_type;

-- Insert one genuinely late event
insert into events (event_id,event_type,order_id,customer_id,amount,event_time,ingestion_time,source_offset,
    batch_id,is_late
)
values (
    convert(varchar(36), NEWID()),
    'order_created',
    'ORD_LATE_001',
    'CUST001',
    250.00,
    DATEADD(MINUTE, -15, SYSUTCDATETIME()),
    SYSUTCDATETIME(),
    101,
    'batch_002',
    1
);


select event_id,event_type,event_time,ingestion_time,source_offset,is_late
from dbo.events
where source_offset = 101;


select last_offset as checkpoints
from pipeline_checkpoints
where source_name = 'ecommerce_events';

select SUM(event_count) as TotalProcessedEvents
from minute_aggregates;


create table pipeline_watermarks (
    source_name varchar(100) primary key,
    max_event_time datetime2 NOT NULL,
    watermark_time datetime2 NOT NULL,
    allowed_lateness_minutes int NOT NULL,
    updated_at datetime2 default SYSUTCDATETIME()
);

-- Initialize the watermark from existing events
insert into pipeline_watermarks (
    source_name,
    max_event_time,
    watermark_time,
    allowed_lateness_minutes
)

select
    'ecommerce_events',
    max(event_time),
    dateadd(MINUTE, -5, max(event_time)),
    5
from events;

select source_name,max_event_time,watermark_time,allowed_lateness_minutes
from pipeline_watermarks;


select e.source_offset,e.event_time,w.watermark_time,
    case
        when e.event_time < w.watermark_time
        then 'LATE EVENT'
        else 'ON TIME'
    end as event_status
from dbo.events e
CROSS JOIN dbo.pipeline_watermarks w
where e.source_offset = 101 and w.source_name = 'ecommerce_events';

-- Test a genuinely late event against the watermark
declare @OldEventTime datetime2 =
    dateadd(MINUTE, -15, SYSUTCDATETIME());

insert into dbo.events (
    event_id,
    event_type,
    order_id,
    customer_id,
    amount,
    event_time,
    ingestion_time,
    source_offset,
    batch_id,
    is_late
)
values (
    convert(VARCHAR(36), NEWID()),
    'order_created',
    'ORD_LATE_002',
    'CUST002',
    450.00,
    @OldEventTime,
    SYSUTCDATETIME(),
    102,
    'batch_003',
    1
);

--— Check its watermark status
select e.source_offset,e.event_time,w.watermark_time,
    case
        when e.event_time < w.watermark_time
        then 'LATE EVENT'
        else 'ON TIME'
    end AS event_status
from dbo.events e
CROSS JOIN dbo.pipeline_watermarks w
where e.source_offset = 102 and w.source_name = 'ecommerce_events';


USE RealtimeEcommerceDB;
GO

UPDATE dbo.pipeline_watermarks
SET
    max_event_time = (
        SELECT MAX(event_time)
        FROM dbo.events
    ),
    watermark_time = DATEADD(
        MINUTE,
        -5,
        (SELECT MAX(event_time) FROM dbo.events)
    ),
    updated_at = SYSUTCDATETIME()
WHERE source_name = 'ecommerce_events';
GO

SELECT
    source_name,
    max_event_time,
    watermark_time
FROM dbo.pipeline_watermarks;


DECLARE @NewEventTime DATETIME2;

SELECT @NewEventTime = DATEADD(
    MINUTE, 30, MAX(event_time)
)
FROM dbo.events;

insert into dbo.events
(
    event_id, event_type, order_id, customer_id, amount, event_time, ingestion_time, source_offset, batch_id, is_late
)
values
(
    NEWID(),'order_created','ORD_NEW_003','CUST003',600.00,@NewEventTime,@NewEventTime,103,'batch_004',0
);


update w
set
    max_event_time = m.max_event_time,
    watermark_time = DATEADD(MINUTE, -5, m.max_event_time),
    updated_at = SYSUTCDATETIME()
from dbo.pipeline_watermarks w
CROSS APPLY
(
    select MAX(event_time) AS max_event_time
    from dbo.events
) m
where w.source_name = 'ecommerce_events'
AND m.max_event_time > w.max_event_time;

select e.source_offset,e.event_time,w.watermark_time,
    case
        when e.event_time < w.watermark_time
        then 'LATE EVENT'
        else 'ON TIME'
    end as event_status
from dbo.events e
CROSS JOIN dbo.pipeline_watermarks w
where e.source_offset = 102 and w.source_name = 'ecommerce_events';

select *
from dbo.pipeline_checkpoints
where source_name = 'ecommerce_events';

select sum(event_count) as TotalAggregatedEvents
from dbo.minute_aggregates;

select source_offset,event_time,is_late,batch_id
from dbo.events
where source_offset IN (101, 102, 103)
order by source_offset;

select source_name,last_offset
from dbo.pipeline_checkpoints;

SELECT
    source_offset,
    COUNT(*) AS RecordCount
FROM dbo.events
GROUP BY source_offset
HAVING COUNT(*) > 1
ORDER BY source_offset;

SELECT event_id,source_offset, event_type, amount, event_time, ingestion_time, batch_id, is_late
FROM dbo.events
WHERE source_offset IN (101, 102, 103)
ORDER BY source_offset, event_time;

SELECT
    (SELECT COUNT(*) FROM dbo.events) AS TotalSourceEvents,
    (SELECT COUNT(DISTINCT event_id) FROM dbo.events) AS UniqueEventIDs,
    (SELECT COUNT(DISTINCT source_offset) FROM dbo.events) AS UniqueOffsets,
    (SELECT SUM(event_count) FROM dbo.minute_aggregates) AS TotalAggregatedEvents;

SELECT
    COUNT(*) AS TotalEvents,
    COUNT(DISTINCT event_id) AS UniqueEventIDs,
    COUNT(DISTINCT source_offset) AS UniqueOffsets
FROM dbo.events;

--Reconcile minute aggregates
begin transaction;

delete from minute_aggregates;

insert into minute_aggregates
(
    window_start, event_type,event_count,total_amount,updated_at
)
select
    dateadd(
        MINUTE,
        DATEDIFF(MINUTE, 0, event_time),
        0
    ) AS window_start,
    event_type,
    count(*) AS event_count,
    sum(COALESCE(amount, 0)) AS total_amount,
    SYSUTCDATETIME()
from dbo.events
group by
    DATEADD(
        MINUTE,
        DATEDIFF(MINUTE, 0, event_time),
        0
    ),
    event_type;

commit transaction;

select sum(event_count) as TotalAggregatedEvents
from dbo.minute_aggregates;



SELECT SUM(event_count) AS TotalAggregatedEvents
FROM dbo.minute_aggregates;

-- Check for duplicate source offsets
select
    source_offset,COUNT(*) AS duplicate_count
from events
group by source_offset
having COUNT(*) > 1;

-- Check the current checkpoint
select *
from pipeline_checkpoints
where source_name = 'ecommerce_events';

-- Compare event count and aggregate count
select
    (select COUNT(*) FROM dbo.events) AS total_events,
    (select COALESCE(SUM(event_count), 0)
     from minute_aggregates) as total_aggregated_events;

create unique index UX_events_source_offset
on dbo.events(source_offset);

select name, type_desc, is_unique, is_disabled
from sys.indexes
where object_id = OBJECT_ID('dbo.events')
and name = 'UX_events_source_offset';

-- 1. Verify source events
SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_id) AS unique_event_ids,
    COUNT(DISTINCT source_offset) AS unique_offsets
FROM dbo.events;

-- 2. Verify rebuilt aggregates
SELECT
    COUNT(*) AS aggregate_groups,
    SUM(event_count) AS total_aggregated_events
FROM dbo.minute_aggregates;

-- 3. Verify checkpoint
SELECT *
FROM dbo.pipeline_checkpoints
WHERE source_name = 'ecommerce_events';

-- 4. Verify watermark
SELECT *
FROM dbo.pipeline_watermarks
WHERE source_name = 'ecommerce_events';

WITH source_summary AS (
    SELECT
        event_type,
        COUNT(*) AS event_count,
        SUM(COALESCE(amount, 0)) AS total_amount
    FROM dbo.events
    GROUP BY event_type
),
aggregate_summary AS (
    SELECT
        event_type,
        SUM(event_count) AS event_count,
        SUM(total_amount) AS total_amount
    FROM dbo.minute_aggregates
    GROUP BY event_type
)
SELECT
    COALESCE(s.event_type, a.event_type) AS event_type,
    COALESCE(s.event_count, 0) AS source_count,
    COALESCE(a.event_count, 0) AS aggregate_count,
    COALESCE(s.total_amount, 0) AS source_amount,
    COALESCE(a.total_amount, 0) AS aggregate_amount,
    COALESCE(s.event_count, 0)
        - COALESCE(a.event_count, 0) AS count_difference,
    COALESCE(s.total_amount, 0)
        - COALESCE(a.total_amount, 0) AS amount_difference
FROM source_summary s
FULL OUTER JOIN aggregate_summary a
    ON s.event_type = a.event_type
ORDER BY event_type;

SELECT TOP 10
    run_id,
    batch_id,
    started_at,
    completed_at,
    status,
    records_read,
    records_inserted,
    records_rejected,
    error_message
FROM dbo.pipeline_runs
ORDER BY started_at DESC;

SELECT COUNT(*) AS total_events
FROM dbo.events;


SELECT
    COUNT(*) AS TotalEvents,
    MIN(source_offset) AS FirstOffset,
    MAX(source_offset) AS LastOffset,
    COUNT(DISTINCT source_offset) AS UniqueOffsets
FROM dbo.events;

SELECT *
FROM dbo.pipeline_checkpoints
WHERE source_name = 'ecommerce_events';

SELECT
    event_type,
    COUNT(*) AS SourceCount,
    SUM(COALESCE(amount, 0)) AS SourceAmount
FROM dbo.events
GROUP BY event_type;

SELECT
    event_type,
    SUM(event_count) AS AggregatedCount,
    SUM(total_amount) AS AggregatedAmount
FROM dbo.minute_aggregates
GROUP BY event_type;

WITH SourceData AS (
    SELECT
        DATEADD(
            MINUTE,
            DATEDIFF(MINUTE, 0, event_time),
            0
        ) AS window_start,
        event_type,
        COUNT(*) AS source_count,
        SUM(COALESCE(amount, 0)) AS source_amount
    FROM dbo.events
    GROUP BY
        DATEADD(
            MINUTE,
            DATEDIFF(MINUTE, 0, event_time),
            0
        ),
        event_type
)
SELECT
    s.window_start,
    s.event_type,
    s.source_count,
    COALESCE(a.event_count, 0) AS aggregate_count,
    s.source_amount,
    COALESCE(a.total_amount, 0) AS aggregate_amount
FROM SourceData s
LEFT JOIN dbo.minute_aggregates a
    ON s.window_start = a.window_start
    AND s.event_type = a.event_type
WHERE
    s.source_count <> COALESCE(a.event_count, 0)
    OR s.source_amount <> COALESCE(a.total_amount, 0)
ORDER BY s.window_start;

SELECT
    event_type,
    SUM(event_count) AS TotalEvents,
    SUM(total_amount) AS TotalAmount
FROM dbo.minute_aggregates
GROUP BY event_type
ORDER BY event_type;


USE RealtimeEcommerceDB;
GO

INSERT INTO dbo.processed_events (
    event_id,
    source_offset
)
SELECT
    e.event_id,
    e.source_offset
FROM dbo.events e
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.processed_events p
    WHERE p.event_id = e.event_id
);
GO

SELECT
    COUNT(*) AS TotalProcessedEvents,
    COUNT(DISTINCT event_id) AS UniqueEventIDs,
    COUNT(DISTINCT source_offset) AS UniqueOffsets
FROM dbo.processed_events;

SELECT
    (SELECT COUNT(*) FROM dbo.events) AS TotalEvents,
    (SELECT COUNT(*) FROM dbo.processed_events) AS ProcessedEvents,
    (SELECT COUNT(*) FROM dbo.minute_aggregates) AS AggregateGroups;
GO

SELECT * FROM pipeline_checkpoints;
GO

SELECT
    SUM(event_count) AS TotalAggregatedEvents,
    SUM(total_amount) AS TotalAggregatedAmount,
    COUNT(*) AS AggregateGroups
FROM dbo.minute_aggregates;


SELECT
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'pipeline_checkpoints';
GO

UPDATE dbo.pipeline_checkpoints
SET
    last_offset = 0,
    updated_at = SYSUTCDATETIME()
WHERE source_name = 'ecommerce_events';
GO

SELECT *
FROM dbo.pipeline_checkpoints;

SELECT
    (SELECT COUNT(*) FROM dbo.events) AS TotalEvents,
    (SELECT COUNT(*) FROM dbo.processed_events) AS ProcessedEvents,
    (SELECT COUNT(*) FROM dbo.minute_aggregates) AS AggregateGroups;
GO

SELECT
    SUM(event_count) AS TotalAggregatedEvents,
    SUM(total_amount) AS TotalAggregatedAmount,
    COUNT(*) AS AggregateGroups
FROM minute_aggregates;
GO

SELECT *
FROM pipeline_checkpoints;

EXEC sp_help 'dbo.events';

SELECT
    MAX(source_offset) AS LatestOffset,
    COUNT(*) AS TotalEvents
FROM dbo.events;

INSERT INTO dbo.events (
    event_id,
    event_type,
    order_id,
    customer_id,
    amount,
    event_time,
    ingestion_time,
    source_offset,
    batch_id,
    is_late
)
VALUES (
    'TEST_EXACTLY_ONCE_001',
    'order_created',
    'TEST_ORDER_001',
    'TEST_CUSTOMER_001',
    500.00,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    1104,
    'EXACTLY_ONCE_TEST',
    0
);

SELECT *
FROM dbo.events
WHERE event_id = 'TEST_EXACTLY_ONCE_001';


SELECT *
FROM dbo.processed_events
WHERE event_id = 'TEST_EXACTLY_ONCE_001';
GO

SELECT *
FROM dbo.processed_events
WHERE event_id = 'TEST_EXACTLY_ONCE_001';

SELECT *
FROM dbo.pipeline_checkpoints
WHERE source_name = 'ecommerce_events';

SELECT
    SUM(event_count) AS TotalAggregatedEvents,
    SUM(total_amount) AS TotalAggregatedAmount,
    COUNT(*) AS AggregateGroups
FROM dbo.minute_aggregates;
GO

SELECT *
FROM dbo.processed_events
WHERE event_id = 'TEST_EXACTLY_ONCE_001';

SELECT
    SUM(event_count) AS TotalAggregatedEvents,
    SUM(total_amount) AS TotalAggregatedAmount,
    COUNT(*) AS AggregateGroups
FROM dbo.minute_aggregates;

SELECT *
FROM dbo.pipeline_checkpoints
WHERE source_name = 'ecommerce_events';

SELECT
    SUM(event_count) AS TotalAggregatedEvents,
    SUM(total_amount) AS TotalAggregatedAmount,
    COUNT(*) AS AggregateGroups
FROM dbo.minute_aggregates;

SELECT
    (SELECT COUNT(*) FROM dbo.events) AS TotalEvents,
    (SELECT COUNT(*) FROM dbo.processed_events) AS ProcessedEvents,
    (SELECT COUNT(*) FROM dbo.minute_aggregates) AS AggregateGroups;

SELECT
    SUM(event_count) AS TotalAggregatedEvents,
    SUM(total_amount) AS TotalAggregatedAmount
FROM dbo.minute_aggregates;

SELECT *
FROM dbo.pipeline_checkpoints
WHERE source_name = 'ecommerce_events';
GO


INSERT INTO dbo.events (
    event_id,
    event_type,
    order_id,
    customer_id,
    amount,
    event_time,
    ingestion_time,
    source_offset,
    batch_id,
    is_late
)
VALUES (
    'TEST_CRASH_RECOVERY_001',
    'order_created',
    'TEST_CRASH_ORDER_001',
    'TEST_CRASH_CUSTOMER_001',
    750.00,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    1105,
    'CRASH_RECOVERY_TEST',
    0
);
GO

SELECT *
FROM dbo.events
WHERE event_id = 'TEST_CRASH_RECOVERY_001';

SELECT *
FROM dbo.processed_events
WHERE event_id = 'TEST_CRASH_RECOVERY_001';


-- 1. Check whether the test event was registered
SELECT *
FROM dbo.processed_events
WHERE event_id = 'TEST_CRASH_RECOVERY_001';

-- 2. Check aggregate totals
SELECT
    SUM(event_count) AS total_aggregated_events,
    SUM(total_amount) AS total_aggregated_amount,
    COUNT(*) AS aggregate_groups
FROM dbo.minute_aggregates;

-- 3. Check checkpoint
SELECT source_name, last_offset
FROM dbo.pipeline_checkpoints
WHERE source_name = 'ecommerce_events';

-- 4. Check source event still exists
SELECT COUNT(*) AS source_event_count
FROM dbo.events
WHERE event_id = 'TEST_CRASH_RECOVERY_001';
GO

SELECT COUNT(*) AS source_event_count
FROM dbo.events
WHERE event_id = 'TEST_CRASH_RECOVERY_001';

SELECT
    (SELECT COUNT(*) FROM dbo.events) AS source_events,

    (SELECT COUNT(*) FROM dbo.processed_events) AS ledger_events,

    (SELECT SUM(event_count)
     FROM dbo.minute_aggregates) AS aggregated_events,

    (SELECT SUM(total_amount)
     FROM dbo.minute_aggregates) AS aggregated_amount,

    (SELECT last_offset
     FROM dbo.pipeline_checkpoints
     WHERE source_name = 'ecommerce_events') AS checkpoints;
GO

-- Inspect source events
EXEC sp_help 'dbo.events';

-- Inspect processing ledger
EXEC sp_help 'dbo.processed_events';

-- Inspect aggregate table
EXEC sp_help 'dbo.minute_aggregates';
GO

SELECT
    event_type,
    COUNT(*) AS total_events,

    SUM(CASE
        WHEN amount IS NULL THEN 1
        ELSE 0
    END) AS null_amounts,

    SUM(CASE
        WHEN amount < 0 THEN 1
        ELSE 0
    END) AS negative_amounts,

    SUM(CASE
        WHEN amount = 0 THEN 1
        ELSE 0
    END) AS zero_amounts,

    SUM(CASE
        WHEN amount IS NOT NULL THEN 1
        ELSE 0
    END) AS populated_amounts

FROM dbo.events
GROUP BY event_type
ORDER BY event_type;
GO

INSERT INTO dbo.events (
    event_id,
    event_type,
    order_id,
    customer_id,
    amount,
    event_time,
    ingestion_time,
    source_offset,
    batch_id,
    is_late
)
VALUES (
    'TEST_INVALID_AMOUNT_001',
    'order_created',
    'TEST_ORDER_001',
    'TEST_CUSTOMER_001',
    -100.00,
    GETDATE(),
    GETDATE(),
    1106,
    'QUALITY_TEST',
    0
);

SELECT
    event_id,
    event_type,
    amount,
    source_offset
FROM dbo.events
WHERE event_id = 'TEST_INVALID_AMOUNT_001';

DELETE FROM dbo.events
WHERE event_id = 'TEST_INVALID_AMOUNT_001';

SELECT COUNT(*) AS remaining_test_records
FROM dbo.events
WHERE event_id = 'TEST_INVALID_AMOUNT_001';

SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME LIKE '%run%'
   OR TABLE_NAME LIKE '%execution%'
ORDER BY TABLE_NAME, ORDINAL_POSITION;

SELECT
    @@SERVERNAME AS server_name,
    DB_NAME() AS current_database;
GO

SELECT
    name,
    state_desc
FROM sys.databases
WHERE name = 'RealtimeEcommerceDB';
GO

SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
GO

EXEC sp_help 'dbo.pipeline_runs';

SELECT TOP 10 *
FROM dbo.pipeline_runs
ORDER BY 1 DESC;
GO

ALTER TABLE dbo.pipeline_runs
ADD
    batches_processed INT NULL,
    processing_events INT NULL,
    duplicates_skipped INT NULL,
    late_events_detected INT NULL,
    final_checkpoint BIGINT NULL,
    final_watermark DATETIME2(7) NULL,
    processing_duration_seconds DECIMAL(12,4) NULL,
    events_per_second DECIMAL(18,2) NULL,
    ingestion_duration_seconds DECIMAL(12,4) NULL,
    reconciliation_duration_seconds DECIMAL(12,4) NULL,
    total_duration_seconds DECIMAL(12,4) NULL;

EXEC sp_help 'dbo.pipeline_runs';

SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_id) AS unique_event_ids,
    COUNT(DISTINCT source_offset) AS unique_source_offsets
FROM dbo.events;

SELECT
    COUNT(*) AS total_quarantined_records
FROM dbo.event_quarantine;

SELECT
    source_offset,
    COUNT(*) AS duplicate_count
FROM dbo.events
GROUP BY source_offset
HAVING COUNT(*) > 1;