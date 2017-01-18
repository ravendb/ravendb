namespace Raven.Server.NotificationCenter.Alerts
{
    public enum DatabaseAlertType
    {
        SqlReplicationConnectionError,
        SqlReplicationProviderError,
        SqlReplicationSlowSql,
        SqlReplicationConnectionStringMissing,
        SqlReplicationError,
        SqlReplicationWriteErrorRatio,
        SqlReplicationScriptError,
        SqlReplicationScriptErrorRatio,
        PeriodicExport,
        Replication
    }
}