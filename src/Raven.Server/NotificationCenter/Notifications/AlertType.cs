// ReSharper disable InconsistentNaming
namespace Raven.Server.NotificationCenter.Notifications
{
    public enum AlertType
    {
        SqlEtl_ConnectionError,
        SqlEtl_ProviderError,
        SqlEtl_SlowSql,
        SqlEtl_ConnectionStringMissing,
        SqlEtl_Error,
        SqlEtl_WriteErrorRatio,
        SqlEtl_ScriptError,
        SqlEtl_ScriptErrorRatio,
        PeriodicExport,
        Replication,
        Server_NewVersionAvailable,
        LicenseManager_InitializationError,
        IndexStore_IndexCouldNotBeOpened,
        TransformerStore_TransformerCouldNotBeOpened,
        WarnIndexOutputsPerDocument,
        ErrorSavingReduceOutputDocuments 
    }
}