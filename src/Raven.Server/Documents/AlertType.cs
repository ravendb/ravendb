namespace Raven.Server.Documents
{
    public enum AlertType
    {
        SqlReplicationConnectionError,
        SqlReplicationProviderError,
        SqlReplicationSlowSql,
        SqlReplicationConnectionStringMissing,
        SqlReplicationError,
        SqlReplicationWriteErrorRatio,
        SqlReplicationScriptError,
        SqlReplicationScriptErrorRatio,
        PeriodicExport
    }
}