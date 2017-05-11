// ReSharper disable InconsistentNaming
namespace Raven.Server.NotificationCenter.Notifications
{
    public enum AlertType
    {
        Etl_Error,
        Etl_TransformationError,
        Etl_LoadError,

        SqlEtl_ConnectionError,
        SqlEtl_ProviderError,
        SqlEtl_SlowSql,
        
        Etl_WriteErrorRatio,
        
        PeriodicExport,
        Replication,
        Server_NewVersionAvailable,
        LicenseManager_InitializationError,
        LicenseManager_LeaseLicenseError,
        LicenseManager_LicenseUpdated,
        IndexStore_IndexCouldNotBeOpened,
        TransformerStore_TransformerCouldNotBeCreated,
        WarnIndexOutputsPerDocument,
        ErrorSavingReduceOutputDocuments,
        CatastrophicDatabaseFailure,
        NonDurableFileSystem,
        RecoveryError,

        ClusterTopologyWarning
    }
}