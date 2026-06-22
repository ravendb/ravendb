// ReSharper disable InconsistentNaming

namespace Raven.Server.NotificationCenter.Notifications
{
    public enum AlertReason
    {
        Etl_Error = 0,
        Etl_Warning = 1,
        
        // Backward compatibility
        Etl_TransformationError = 2,
        Etl_LoadError = 3,
        
        QueueSink_Error = 4,
        QueueSink_Warning = 5,
        QueueSink_ScriptError = 6,
        QueueSink_ConsumeError = 7,
        QueueSink_ConsumerCreationError = 8,

        AiAgent_ExceededTokenThreshold = 9,

        SqlEtl_ConnectionError = 10,
        SqlEtl_ProviderError = 11,
        
        SnowflakeEtl_ConnectionError = 12,

        Etl_InvalidScript = 13,

        PeriodicBackup = 14,
        Replication = 15,
        Server_NewVersionAvailable = 16,

        LicenseManager_InitializationError = 17,
        LicenseManager_LeaseLicenseSuccess = 18,
        LicenseManager_LeaseLicenseError = 19,
        LicenseManager_LicenseUpdateMessage = 20,
        LicenseManager_HighlyAvailableTasks = 21,
        LicenseManager_LicenseLimit = 22,
        LicenseManager_AGPL3 = 23,

        Certificates_Expiration = 24,
        Certificates_DeveloperLetsEncryptRenewal = 25,
        Certificates_EntireClusterReplaceSuccess = 26,
        Certificates_ReplaceSuccess = 27,
        Certificates_ReplaceError = 28,
        Certificates_ReplacePending = 29,

        IndexStore_IndexCouldNotBeOpened = 30,
        WarnIndexOutputsPerDocument = 31,
        ErrorSavingReduceOutputDocuments = 32,
        CatastrophicDatabaseFailure = 33,
        RecoverableVoronFailure = 34,
        NonDurableFileSystem = 35,
        RecoveryError = 36,
        RestoreError = 37,
        DeletionError = 38,

        ClusterTopologyWarning = 39,
        DatabaseTopologyWarning = 40,
        SwappingHddInsteadOfSsd = 41,

        RevisionsConfigurationNotValid = 42,
        ArchivalConfigurationNotValid = 43,

        ReplicationMissingAttachments = 44,

        ClusterTransactionFailure = 45,

        OutOfMemoryException = 46,

        LowDiskSpace = 47,

        // Required for backward compatibility
        UnexpectedIndexingThreadError = 48,

        Indexing_UnexpectedIndexingThreadError = 49,
        Indexing_CouldNotGetStats = 50,
        Indexing_CoraxComplexItem = 51,

        CpuUsageExtensionPointError = 52,
        TcpListenerError = 53,

        Throttling_CpuCreditsBalance = 54,

        IntegrityErrorOfAlreadySyncedData = 55,

        ConcurrentDatabaseLoadTimeout = 56,

        HighClientCreationRate = 57,
        RollupExceedNumberOfValues = 58,

        LowSwapSize = 59,

        UnrecoverableClusterError = 60,
        
        MicrosoftLogsConfigurationLoadError = 61,
        
        MismatchedReferenceLoad = 62,

        BlockingTombstones = 63,
        ServerLimits = 64,

        ConflictRevisionsExceeded = 65,
        
        SqlConnectionString_DeprecatedFactoryReplaced = 66,
        
        Attachments_RemoteAttachmentWithoutIdentifier = 67,
        Attachments_RemoteAttachmentErroredIdentifier = 68,
        RemoteAttachmentsConfigurationNotValid = 69,
        
        SchemaValidationConfiguration_Error = 70,

        GcThreadContention = 71,
        
        Etl_HealthStatusChange = 72,

        HighReadAheadKb = 73 // 72 is used in 7.2
    }
}
