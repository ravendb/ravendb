using System.ComponentModel;

namespace Raven.Server.Documents.Operations;

public enum OperationType
{
    [Description("Setup")]
    Setup,

    [Description("Update by Query")]
    UpdateByQuery,

    [Description("Delete by Query")]
    DeleteByQuery,

    [Description("Database export")]
    DatabaseExport,

    [Description("Database import")]
    DatabaseImport,

    [Description("Collection import from CSV")]
    CollectionImportFromCsv,

    [Description("RavenDB Database migration")]
    DatabaseMigrationRavenDb,

    [Description("Database Restore")]
    DatabaseRestore,

    [Description("Database compact")]
    DatabaseCompact,

    [Description("Index compact")]
    IndexCompact,

    [Description("Delete by collection")]
    DeleteByCollection,

    [Description("Bulk Insert")]
    BulkInsert,

    [Description("Replay Transaction Commands")]
    ReplayTransactionCommands,

    [Description("Record Transaction Commands")]
    RecordTransactionCommands,

    [Description("Certificate generation")]
    CertificateGeneration,

    [Description("Migration from v3.x")]
    MigrationFromLegacyData,

    [Description("Database Backup")]
    DatabaseBackup,

    [Description("Migration from SQL")]
    MigrationFromSql,

    [Description("Database Migration")]
    DatabaseMigration,

    [Description("Database Revert")]
    DatabaseRevert,

    [Description("Enforce Revision Configuration")]
    EnforceRevisionConfiguration,

    [Description("Debug package")]
    DebugPackage,

    [Description("Dump Raw Index Data - Debug")]
    DumpRawIndexData
}
