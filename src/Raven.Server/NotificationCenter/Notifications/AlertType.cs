// ReSharper disable InconsistentNaming
namespace Raven.Server.NotificationCenter.Notifications
{
    public enum AlertType
    {
        Etl_Error,
        Etl_Warning,
        Etl_TransformationError,
        Etl_LoadError,

        SqlEtl_ConnectionError,
        SqlEtl_ProviderError,
        SqlEtl_SlowSql,
        
        Etl_WriteErrorRatio,
        
        PeriodicBackup,
        Replication,
        Server_NewVersionAvailable,
        LicenseManager_InitializationError,
        LicenseManager_LeaseLicenseError,
        LicenseManager_LicenseUpdateMessage,
        LicenseManager_LicenseLimit,
        IndexStore_IndexCouldNotBeOpened,
        TransformerStore_TransformerCouldNotBeCreated,
        WarnIndexOutputsPerDocument,
        ErrorSavingReduceOutputDocuments,
        CatastrophicDatabaseFailure,
        NonDurableFileSystem,
        RecoveryError,
        RestoreError,
        ClusterTopologyWarning,
        DatabaseTopologyWarning,
        SwappingHddInsteadOfSsd,

        RevisionsConfigurationNotValid
    }
}
