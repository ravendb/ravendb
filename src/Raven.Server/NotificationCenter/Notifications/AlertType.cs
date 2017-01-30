// ReSharper disable InconsistentNaming
namespace Raven.Server.NotificationCenter.Notifications
{
    public enum AlertType
    {
        SqlReplication_ConnectionError,
        SqlReplication_ProviderError,
        SqlReplication_SlowSql,
        SqlReplication_ConnectionStringMissing,
        SqlReplication_Error,
        SqlReplication_WriteErrorRatio,
        SqlReplication_ScriptError,
        SqlReplication_ScriptErrorRatio,
        PeriodicExport,
        Replication,
        Server_NewVersionAvailable,
        LicenseManager_InitializationError,
        IndexStore_IndexCouldNotBeOpened,
        TransformerStore_TransformerCouldNotBeOpened
    }
}