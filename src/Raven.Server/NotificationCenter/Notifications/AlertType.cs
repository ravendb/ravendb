﻿// ReSharper disable InconsistentNaming
namespace Raven.Server.NotificationCenter.Notifications
{
    public enum AlertType
    {
        Etl_Error,
        Etl_Warning,
        Etl_TransformationError,
        Etl_LoadError,
        
        QueueSink_Error,
        QueueSink_Warning,
        QueueSink_ScriptError,
        QueueSink_ConsumeError,
        QueueSink_ConsumerCreationError,

        SqlEtl_ConnectionError,
        SqlEtl_ProviderError,

        Etl_InvalidScript,

        PeriodicBackup,
        Replication,
        Server_NewVersionAvailable,

        LicenseManager_InitializationError,
        LicenseManager_LeaseLicenseSuccess,
        LicenseManager_LeaseLicenseError,
        LicenseManager_LicenseUpdateMessage,
        LicenseManager_HighlyAvailableTasks,
        LicenseManager_LicenseLimit,
        LicenseManager_AGPL3,

        Certificates_Expiration,
        Certificates_DeveloperLetsEncryptRenewal,
        Certificates_EntireClusterReplaceSuccess,
        Certificates_ReplaceSuccess,
        Certificates_ReplaceError,
        Certificates_ReplacePending,

        IndexStore_IndexCouldNotBeOpened,
        WarnIndexOutputsPerDocument,
        ErrorSavingReduceOutputDocuments,
        CatastrophicDatabaseFailure,
        RecoverableVoronFailure,
        NonDurableFileSystem,
        RecoveryError,
        RestoreError,
        DeletionError,

        ClusterTopologyWarning,
        DatabaseTopologyWarning,
        SwappingHddInsteadOfSsd,

        RevisionsConfigurationNotValid,
        ArchivalConfigurationNotValid,
        RetireAttachmentsConfigurationNotValid,

        ReplicationMissingAttachments,

        ClusterTransactionFailure,

        OutOfMemoryException,

        LowDiskSpace,

        UnexpectedIndexingThreadError,

        Indexing_UnexpectedIndexingThreadError,
        Indexing_CouldNotGetStats,
        Indexing_CoraxComplexItem,

        CpuUsageExtensionPointError,
        TcpListenerError,

        Throttling_CpuCreditsBalance,

        IntegrityErrorOfAlreadySyncedData,

        ConcurrentDatabaseLoadTimeout,

        HighClientCreationRate,
        RollupExceedNumberOfValues,

        LowSwapSize,

        UnrecoverableClusterError,
        
        MicrosoftLogsConfigurationLoadError,
        
        MismatchedReferenceLoad,

        BlockingTombstones,
        ServerLimits,

        ConflictRevisionsExceeded,
        
        SqlConnectionString_DeprecatedFactoryReplaced,
    }
}
