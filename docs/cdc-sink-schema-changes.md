# CDC Sink: Schema Change Guide for Database Administrators

This guide explains how to safely modify your source database schema while a RavenDB CDC Sink is running. The behavior differs by database provider.

## Quick Reference

| Change | MySQL | PostgreSQL | SQL Server |
|--------|-------|------------|------------|
| Add column (unmapped) | Transparent | Transparent | Requires procedure |
| Add column (mapped) | Update config + restart | Update config + restart | Requires procedure + config update |
| Drop unmapped column | Transparent | Transparent | Requires procedure |
| Drop mapped column | Sink enters fallback (config error) | Sink enters fallback (config error) | Requires procedure |
| Change column type | Sink restarts, usually recovers | Transparent (auto-rebuild) | Requires procedure |
| Rename column | Transparent (if unmapped) | Sink enters fallback (if mapped) | Requires procedure |

---

## MySQL

### How It Works

MySQL CDC uses binlog replication. Each row event is preceded by a `TableMapEvent` containing column type information. The CDC Sink compares the column types at mapped positions against the expected types from `INFORMATION_SCHEMA`. If they don't match, the process detects a schema change.

### Adding an Unmapped Column

No action needed. If the new column is not in your CDC Sink column mappings, it's ignored.

```sql
ALTER TABLE orders ADD COLUMN internal_notes TEXT;
-- CDC Sink continues without interruption
```

### Adding a Mapped Column

1. Add the column to the source table
2. Update the CDC Sink configuration to include the new column mapping
3. The process restarts automatically and picks up the new schema

```sql
ALTER TABLE orders ADD COLUMN priority INT DEFAULT 0;
-- Then update CDC Sink config in RavenDB to map the 'priority' column
```

### Dropping a Mapped Column

This is a **configuration error** — the CDC Sink expects the column to exist. The process will enter fallback mode and keep retrying until the configuration is updated to remove the mapping.

```sql
ALTER TABLE orders DROP COLUMN legacy_field;
-- CDC Sink enters fallback. Update the config to remove 'legacy_field' from column mappings.
```

### Dropping an Unmapped Column

Transparent. No action needed.

```sql
ALTER TABLE orders DROP COLUMN internal_notes;
-- CDC Sink continues without interruption
```

### Changing a Column Type

The process detects the type change via binlog column types, restarts, re-resolves from `INFORMATION_SCHEMA`, and continues.

```sql
ALTER TABLE orders MODIFY COLUMN status VARCHAR(100);
-- CDC Sink detects change, restarts, and recovers automatically
```

---

## PostgreSQL

### How It Works

PostgreSQL logical replication sends `RelationMessage` events inline in the WAL stream whenever the schema changes. The CDC Sink detects these automatically and rebuilds its column mapping on the fly. This makes PostgreSQL the most resilient to schema changes.

### Adding an Unmapped Column

Transparent. PostgreSQL sends an updated `RelationMessage`, the sink rebuilds and ignores the new column.

```sql
ALTER TABLE orders ADD COLUMN internal_notes TEXT;
-- CDC Sink auto-rebuilds and continues
```

### Adding a Mapped Column

1. Add the column to the source table
2. Update the CDC Sink configuration to include the new column mapping
3. The process auto-detects the schema change and restarts

```sql
ALTER TABLE orders ADD COLUMN priority INT DEFAULT 0;
-- Update CDC Sink config to map 'priority', process recovers automatically
```

### Dropping a Mapped Column

The sink will enter fallback — it can't find the mapped column in the new schema. Update the configuration to remove the mapping.

```sql
ALTER TABLE orders DROP COLUMN legacy_field;
-- CDC Sink enters fallback. Remove 'legacy_field' from the column mappings.
```

### Dropping an Unmapped Column

Transparent.

```sql
ALTER TABLE orders DROP COLUMN internal_notes;
-- CDC Sink auto-rebuilds and continues
```

### Changing a Column Type

Usually transparent. PostgreSQL sends a new `RelationMessage` and the sink rebuilds its type categories.

```sql
ALTER TABLE orders ALTER COLUMN status TYPE VARCHAR(100);
-- CDC Sink auto-rebuilds and continues
```

### Renaming a Mapped Column

The sink will enter fallback — the old column name is no longer found. Update the configuration.

```sql
ALTER TABLE orders RENAME COLUMN name TO full_name;
-- CDC Sink enters fallback. Update column mapping from 'name' to 'full_name'.
```

### Replica Identity for Embedded Tables

If you use embedded arrays with join columns that are NOT part of the primary key, PostgreSQL must be configured to send all columns on DELETE:

```sql
ALTER TABLE order_lines REPLICA IDENTITY FULL;
```

Without this, DELETE events only include PK columns, and the sink can't route the delete to the correct parent document.

---

## SQL Server

### How It Works

SQL Server CDC uses **capture instances** — immutable snapshots of the table schema at the time CDC was enabled. When you change the schema, you must create a new capture instance. The CDC Sink detects dropped capture instances (via `fn_cdc_get_min_lsn` returning all zeros) and restarts to pick up the new one.

**Important:** SQL Server supports at most **two** capture instances per table at a time. You must drain the old one before creating a third.

### The Schema Change Procedure

For ANY schema change on a CDC-tracked table, follow this procedure:

```sql
-- Step 1: ALTER the table
ALTER TABLE dbo.orders ADD priority INT DEFAULT 0;

-- Step 2: Create a new capture instance (with explicit name for clarity)
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @capture_instance = N'dbo_orders_v2',
    @role_name = NULL;

-- Step 3: WAIT — let the CDC Sink drain the old capture instance.
-- The sink picks the oldest instance and processes all remaining changes.
-- Monitor the RavenDB CDC Sink status to confirm it has caught up.

-- Step 4: Drop the old capture instance
EXEC sys.sp_cdc_disable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @capture_instance = N'dbo_orders';

-- The CDC Sink detects the drop, restarts, and picks up dbo_orders_v2.
```

### What Happens at Each Step

1. **After Step 2:** Two capture instances exist. The CDC Sink continues reading from the **oldest** one (`dbo_orders`). New changes are captured by both instances.

2. **After Step 3 (waiting):** The sink has processed all changes from the old instance. It's safe to drop it.

3. **After Step 4:** `fn_cdc_get_min_lsn('dbo_orders')` returns all zeros. The sink detects this on the next poll cycle, throws an error, enters fallback, and restarts. On restart, it resolves `dbo_orders_v2` as the active capture instance and continues.

### Adding an Unmapped Column

Follow the full procedure above. The new column will be in the capture instance's column list but ignored by the sink (not in the column mappings).

### Adding a Mapped Column

Follow the full procedure above, then update the CDC Sink configuration to include the new column mapping.

### Dropping a Column

Follow the full procedure. If the dropped column was mapped, also update the CDC Sink configuration.

### Verify SQL Server Agent Is Running

CDC requires SQL Server Agent to be running. The capture job (which populates change tables) and cleanup job (which purges old changes) are Agent jobs.

```sql
-- Check Agent status
SELECT * FROM sys.dm_server_services WHERE servicename LIKE '%Agent%';

-- For Docker:
-- docker run -e 'MSSQL_AGENT_ENABLED=true' ...
```

Without the Agent, changes are never captured and the sink polls indefinitely without finding data.

---

## Recovery From Errors

All three providers use the same recovery mechanism:

1. **Error detected** → process enters fallback mode (exponential backoff starting at 5 seconds)
2. **After backoff** → process restarts: re-resolves schema, re-opens connections
3. **If error persists** (e.g., mapped column still missing) → keeps retrying with increasing backoff
4. **RavenDB notification** raised for each error — visible in the Studio

### Checking CDC Sink Health

The CDC Sink status is available in the RavenDB Studio under **Tasks → CDC Sinks**. Look for:
- **Healthy**: processing normally
- **Error recovery**: in fallback mode, will retry
- **Last error**: description of what went wrong

### Resetting After Configuration Fix

After fixing the CDC Sink configuration (e.g., removing a dropped column from mappings), the process will automatically recover on its next retry cycle. No manual restart is needed.
