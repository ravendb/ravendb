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
        LicenseManager_LeaseLicenseSuccess,
        LicenseManager_LeaseLicenseError,
        LicenseManager_LicenseUpdateMessage,
        LicenseManager_HighlyAvailableTasks,
        LicenseManager_LicenseLimit,

        IndexStore_IndexCouldNotBeOpened,
        WarnIndexOutputsPerDocument,
        ErrorSavingReduceOutputDocuments,
        CatastrophicDatabaseFailure,
        NonDurableFileSystem,
        RecoveryError,
        RestoreError,
        DeletionError,

        ClusterTopologyWarning,
        DatabaseTopologyWarning,
        SwappingHddInsteadOfSsd,

        RevisionsConfigurationNotValid
    }
}
