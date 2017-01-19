namespace Raven.Server.NotificationCenter.Alerts
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
        PeriodicExport,
        Replication,
        NewServerVersionAvailable,
        LicenseManagerInitializationError
    }
}