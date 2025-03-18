using System;
using System.ComponentModel;

namespace Tests.Infrastructure;

[Flags]
public enum RavenTestCategory : long
{
    None = 0,
    Attachments = 1L << 1,
    BackupExportImport = 1L << 2,
    BulkInsert = 1 << 3 | ClientApi,
    Certificates = 1L << 4,
    [Description("Client API")]
    ClientApi = 1L << 5,
    Cluster = 1L << 6,
    Compression = 1L << 7,
    Configuration = 1L << 8,
    Counters = 1L << 9,
    [Description("ETL")]
    Etl = 1L << 10,
    Encryption = 1L << 11,
    ExpirationRefresh = 1L << 12,
    Facets = 1L << 13,
    Indexes = 1L << 14,
    JavaScript = 1L << 15,
    Licensing = 1L << 16,
    Linux = 1L << 17,
    Logging = 1L << 18,
    Memory = 1L << 19,
    [Description("PAL")]
    Pal = 1L << 20,
    Patching = 1L << 21,
    [Description("PostgreSQL")]
    PostgreSql = 1L << 22,
    [Description("Power BI")]
    PowerBi = 1L << 23,
    Querying = 1L << 24,
    [Description("RQL")]
    Rql = 1L << 25,
    Replication = 1L << 26,
    Revisions = 1L << 27,
    Setup = 1L << 28,
    Sharding = 1L << 29,
    Spatial = 1L << 30,
    Studio = 1L << 31,
    Subscriptions = 1L << 32,
    TimeSeries = 1L << 33,
    Voron = 1L << 34,
    Windows = 1L << 35,
    Corax = 1L << 36,
    CompareExchange = 1L << 37,
    Embedded = 1L << 38,
    ClusterTransactions = 1L << 39,
    Highlighting = 1L << 40,
    Smuggler = 1L << 41,
    Lucene = 1L << 42,
    [Description("Changes API")]
    ChangesApi = 1L << 43,
    Interversion = 1L << 44,
    Security = 1L << 45,
    Core = 1L << 46,
    Intrinsics = 1L << 47,
    Sinks = 1L << 48,
    Monitoring = 1L << 50,
    Vector = 1L << 51,
}
