# CDC Sink Feature — Documentation Brief

**RavenDB 7.2 — RavenDB-26046**

## What is CDC Sink?

A new ongoing task type that reads Change Data Capture (CDC) streams from PostgreSQL or SQL Server and writes the resulting documents into RavenDB. It provides real-time data synchronization from relational databases into RavenDB documents.

## Supported Databases

- **PostgreSQL** — uses logical replication (streaming, low-latency)
- **SQL Server** — uses native CDC tables (polling-based)

---

## Task Configuration

### Creating a CDC Sink Task

```csharp
var config = new CdcSinkConfiguration
{
    Name = "my-cdc-task",
    ConnectionStringName = "pg-connection",
    Tables = new List<CdcSinkTableConfig>
    {
        new CdcSinkTableConfig
        {
            Name = "Orders",                      // RavenDB collection name
            SourceTableSchema = "public",
            SourceTableName = "orders",
            PrimaryKeyColumns = ["order_id"],
            Columns = new List<CdcColumnMapping>
            {
                new() { Column = "order_id",  Name = "OrderId" },
                new() { Column = "customer",  Name = "Customer" },
                new() { Column = "total",     Name = "Total" },
                new() { Column = "metadata",  Name = "Metadata",     Type = CdcColumnType.Json },
                new() { Column = "receipt",   Name = "receipt.pdf",  Type = CdcColumnType.Attachment },
            }
        }
    }
};

store.Maintenance.Send(new AddCdcSinkOperation(config));
```

### Top-Level Configuration Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `Name` | string | Yes | Unique task name |
| `ConnectionStringName` | string | Yes | SQL connection string name (must already exist in database record) |
| `Tables` | List | Yes | At least one table mapping |
| `Postgres` | CdcSinkPostgresSettings | No | PostgreSQL-specific settings (auto-filled if omitted) |
| `SkipInitialLoad` | bool | No | When true, skips the full-table scan and starts streaming CDC changes immediately. Use when the target database is already populated |
| `Disabled` | bool | No | Disable without deleting |
| `MentorNode` | string | No | Preferred node |
| `PinToMentorNode` | bool | No | Force to mentor node |

---

## Column Mapping

Each table has a `Columns` list where each entry maps a SQL column to a RavenDB target:

```csharp
new CdcColumnMapping
{
    Column = "sql_column_name",   // SQL source column
    Name = "DocumentProperty",    // RavenDB target (property name or attachment name)
    Type = CdcColumnType.Default  // How to store it
}
```

### Column Types

| Type | Behavior | Use For |
|------|----------|---------|
| `Default` | Standard type conversion: int→long, decimal→double, date→DateOnly, timestamp→DateTime, uuid→string, varchar/text→string, SQL arrays→JSON arrays. **JSON/JSONB values stored as plain strings** | Most columns |
| `Json` | Parses the string value as a native JSON object or array in the document | PostgreSQL json/jsonb columns, SQL Server nvarchar(max) with JSON |
| `Attachment` | Stores the raw value as a RavenDB attachment. byte[]→binary, string→UTF-8 text, float[]/double[]→raw vector data | Binary blobs, large text, vector embeddings |

### Validation Rules

- Each `Column` name must be unique within a table
- Each `Name` must be unique within a table
- Both `Column` and `Name` are required (non-empty)
- Property names from columns, embedded tables, and linked tables must not conflict

---

## Table Relationships

### Embedded Tables (Nested Objects/Arrays)

Embed child table rows as nested JSON within the parent document:

```csharp
new CdcSinkTableConfig
{
    Name = "Orders",
    SourceTableName = "orders",
    PrimaryKeyColumns = ["order_id"],
    Columns = [ /* ... */ ],
    EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
    {
        new()
        {
            SourceTableName = "order_lines",
            PropertyName = "Lines",              // Property in the parent document
            Type = CdcSinkRelationType.Array,    // Array, Map, or Value
            JoinColumns = ["order_id"],          // FK columns joining to parent
            PrimaryKeyColumns = ["line_id"],     // For matching items during updates
            Columns = [
                new() { Column = "line_id",  Name = "LineId" },
                new() { Column = "product",  Name = "Product" },
                new() { Column = "quantity", Name = "Quantity" },
            ]
        }
    }
}
```

**Relation Types:**
- `Array` — one-to-many: `{ "Lines": [ { "Product": "Widget" }, ... ] }`
- `Map` — one-to-many keyed by PK: `{ "Attributes": { "color": { "Value": "red" } } }`
- `Value` — many-to-one: `{ "ShippingInfo": { "Carrier": "FedEx" } }`

Embedded tables support deep nesting (embedded within embedded) and can have their own `Patch`, `OnDelete`, `Columns`, and `CaseSensitiveKeys` settings.

### Linked Tables (Document References)

Generate document ID references from foreign keys:

```csharp
LinkedTables = new List<CdcSinkLinkedTableConfig>
{
    new()
    {
        SourceTableName = "customers",
        PropertyName = "Customer",
        LinkedCollectionName = "Customers",     // Target collection
        Type = CdcSinkRelationType.Value,
        JoinColumns = ["customer_id"],
    }
}
// Result: { "Customer": "Customers/42" }
```

Linked tables support `Value` (single reference) and `Array` (array of references). `Map` is not supported for links.

---

## Patch Scripts

JavaScript patches run on the document after column mapping:

### Root Table Patch

```csharp
Patch = @"
    this.FullName = $row.first_name + ' ' + $row.last_name;
    this.UpdatedAt = new Date().toISOString();
"
```

Variables: `this` = the document, `$row` = raw CDC row (all SQL columns), `$old` = previous document state (null for inserts).

### Embedded Table Patch

Same variables but `this` = the **parent** document. Runs after the embedded operation is applied. `$old` provides the previous embedded item for delta computations:

```csharp
Patch = "this.Total += $row.Amount - ($old?.Amount || 0);"
```

### OnDelete Patch

Runs when a DELETE event is received, before the delete is applied:

```csharp
OnDelete = new CdcSinkOnDeleteConfig
{
    Patch = @"
        put('DeletedOrders/' + this.OrderId, {
            OriginalId: id(this),
            Customer: this.Customer,
            Total: this.Total,
            DeletedAt: new Date().toISOString(),
            '@metadata': { '@collection': 'DeletedOrders' }
        });"
}
```

The patch has access to all document properties via `this` and can call `put()`, `del()`, etc. If `IgnoreDeletes = true`, the patch runs but the delete is skipped (archive pattern).

---

## Delete Handling

| Configuration | Behavior |
|---------------|----------|
| `OnDelete = null` (default) | Document/item deleted normally |
| `OnDelete.Patch` only | Patch runs (e.g., audit log), then delete proceeds |
| `OnDelete.IgnoreDeletes = true` | Delete skipped, document survives. Patch modifications persist |
| `IgnoreDeletes + Patch` | Archive pattern: patch marks document (e.g., `this.Archived = true`), delete skipped |

---

## PostgreSQL-Specific Settings

```csharp
Postgres = new CdcSinkPostgresSettings
{
    PublicationName = "my_publication",  // Optional, auto-generated if null
    SlotName = "my_slot",               // Optional, auto-generated if null
}
```

- **Auto-fill:** If omitted, names are auto-generated as `rvn_cdc_p_{guid}` / `rvn_cdc_s_{guid}` on task creation
- **Immutable:** Once set, publication and slot names cannot be changed
- **Verification:** On startup, the process verifies the publication covers all configured tables and the slot uses the `pgoutput` plugin
- **Auto-fix:** If configured tables are missing from the publication, the process attempts `ALTER PUBLICATION ... ADD TABLE` automatically

**Warning on deletion:** Deleting a CDC Sink task does NOT remove the PostgreSQL replication slot and publication. They must be dropped manually by a database administrator. The Studio shows a warning dialog when deleting.

---

## Initial Load

When a CDC Sink task starts, it performs a full-table scan (initial load) for each configured table before switching to CDC streaming. Per-table progress is tracked — if interrupted, the load resumes from the last processed key.

- `SkipInitialLoad = true` marks all tables as loaded immediately
- Adding a new table to an existing task triggers initial load only for the new table
- Tables already loaded are skipped on subsequent restarts

---

## Error Handling

- Individual document processing errors are tolerated (logged, recorded)
- The CDC position (LSN) advances as long as the error ratio stays acceptable
- **Error threshold:** When cumulative errors reach 100 AND exceed the number of successes, the batch throws and the process enters fallback mode
- **Fallback:** Exponential backoff starting at 5 seconds, doubling up to `CdcSink.MaxFallbackTimeInSec` (default 15 minutes)
- Patch errors (including MaxSteps exceeded) fail only the affected document, not the entire batch

---

## Server Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `CdcSink.MaxBatchSize` | 1024 | Target batch size. May exceed if a source transaction has more rows |
| `CdcSink.MaxFallbackTimeInSec` | 900 (15 min) | Max retry backoff after failures |
| `CdcSink.PollIntervalInSec` | 1 | SQL Server polling interval. PostgreSQL ignores this |

---

## REST API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| PUT | `/databases/{db}/admin/cdc-sink` | DatabaseAdmin | Create CDC Sink task |
| PUT | `/databases/{db}/admin/cdc-sink?id={taskId}` | DatabaseAdmin | Update CDC Sink task |
| POST | `/databases/{db}/admin/cdc-sink/test` | DatabaseAdmin | Test a patch script |
| POST | `/databases/{db}/admin/cdc-sink/verify` | DatabaseAdmin | Verify source database connectivity and CDC setup |
| GET | `/databases/{db}/cdc-sink/performance` | ValidUser | Get performance stats |
| GET | `/databases/{db}/cdc-sink/performance/live` | ValidUser | WebSocket for live performance stats |

---

## Type Conversion Reference

### PostgreSQL → RavenDB

| PostgreSQL Type | RavenDB Type | Notes |
|-----------------|-------------|-------|
| `integer`, `smallint`, `bigint` | `long` | |
| `real`, `double precision` | `double` | |
| `numeric`, `decimal` | `double` | |
| `boolean` | `bool` | |
| `date` | `DateOnly` | Serialized as "1990-06-15" |
| `timestamp`, `timestamptz` | `DateTime` | |
| `uuid` | `string` | |
| `varchar`, `text` | `string` | |
| `json`, `jsonb` | `string` or native JSON | Native JSON only when `Type = CdcColumnType.Json` |
| `text[]`, `int[]`, etc. | JSON array | |
| `vector(N)` (pgvector) | JSON array of numbers | Or raw bytes if `Type = Attachment` |
| `bytea` | Base64 string | Or binary attachment if `Type = Attachment` |
| `inet`, `tsvector`, etc. | `string` | ToString() fallback |

### SQL Server → RavenDB

| SQL Server Type | RavenDB Type |
|-----------------|-------------|
| `int`, `smallint`, `bigint`, `tinyint` | `long` |
| `float`, `real` | `double` |
| `decimal`, `numeric` | `double` |
| `bit` | `bool` |
| `datetime`, `datetime2`, `date` | `DateTime` |
| `uniqueidentifier` | `string` |
| `varchar`, `nvarchar`, `text` | `string` |
| `varbinary`, `image` | Base64 string or attachment |

---

## Complete Example

```csharp
var config = new CdcSinkConfiguration
{
    Name = "ecommerce-sync",
    ConnectionStringName = "postgres-prod",
    Tables = new List<CdcSinkTableConfig>
    {
        new()
        {
            Name = "Orders",
            SourceTableSchema = "public",
            SourceTableName = "orders",
            PrimaryKeyColumns = ["order_id"],
            Columns = [
                new() { Column = "order_id",    Name = "OrderId" },
                new() { Column = "customer_id", Name = "CustomerId" },
                new() { Column = "total",       Name = "Total" },
                new() { Column = "metadata",    Name = "Metadata", Type = CdcColumnType.Json },
            ],
            Patch = "this.SyncedAt = new Date().toISOString();",
            OnDelete = new CdcSinkOnDeleteConfig
            {
                Patch = @"put('DeletedOrders/' + this.OrderId, {
                    Customer: this.CustomerId,
                    Total: this.Total,
                    '@metadata': { '@collection': 'DeletedOrders' }
                });"
            },
            LinkedTables = [
                new()
                {
                    SourceTableName = "customers",
                    PropertyName = "Customer",
                    LinkedCollectionName = "Customers",
                    Type = CdcSinkRelationType.Value,
                    JoinColumns = ["customer_id"],
                }
            ],
            EmbeddedTables = [
                new()
                {
                    SourceTableName = "order_lines",
                    PropertyName = "Lines",
                    Type = CdcSinkRelationType.Array,
                    JoinColumns = ["order_id"],
                    PrimaryKeyColumns = ["line_id"],
                    Columns = [
                        new() { Column = "line_id",  Name = "LineId" },
                        new() { Column = "product",  Name = "Product" },
                        new() { Column = "quantity", Name = "Quantity" },
                        new() { Column = "photo",   Name = "product-photo", Type = CdcColumnType.Attachment },
                    ]
                }
            ]
        }
    }
};

store.Maintenance.Send(new AddCdcSinkOperation(config));
```

Resulting document (`Orders/42`):
```json
{
    "OrderId": 42,
    "CustomerId": "ALFKI",
    "Total": 150.0,
    "Metadata": { "source": "web", "priority": "high" },
    "Customer": "Customers/ALFKI",
    "Lines": [
        { "LineId": 1, "Product": "Widget", "Quantity": 3 },
        { "LineId": 2, "Product": "Gadget", "Quantity": 1 }
    ],
    "SyncedAt": "2026-04-03T10:30:00.000Z",
    "@metadata": { "@collection": "Orders", "@attachments": [...] }
}
```

With attachments: `product-photo` on each order line (prefixed as `Lines/1/product-photo`, `Lines/2/product-photo`).
