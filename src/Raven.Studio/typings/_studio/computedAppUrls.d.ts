
type EditPeriodicBackupTaskSourceView = "Backups" | "OngoingTasks";

// Interface
interface computedAppUrls {
    adminSettingsCluster: KnockoutComputed<string>;

    databases: KnockoutComputed<string>;
    clusterDashboard: KnockoutComputed<string>;
    manageDatabaseGroup: KnockoutComputed<string>;
    clientConfiguration: KnockoutComputed<string>;
    studioConfiguration: KnockoutComputed<string>;
    documents: KnockoutComputed<string>;
    revisionsBin: KnockoutComputed<string>;
    conflicts: KnockoutComputed<string>;
    patch: KnockoutComputed<string>;
    cmpXchg: KnockoutComputed<string>;
    identities: KnockoutComputed<string>;
    indexes: KnockoutComputed<string>;
    newIndex: KnockoutComputed<string>;
    editIndex: (indexName?: string) => KnockoutComputed<string>;
    editExternalReplication: (taskId?: number) => KnockoutComputed<string>;
    editReplicationHub: (taskId?: number) => KnockoutComputed<string>;
    editReplicationSink: (taskId?: number) => KnockoutComputed<string>;
    editPeriodicBackupTask: (sourceView: EditPeriodicBackupTaskSourceView, taskId?: number) => KnockoutComputed<string>;
    editSubscription: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editRavenEtl: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editSqlEtl: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editOlapEtl: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editElasticSearchEtl: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editKafkaEtl: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editRabbitMqEtl: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editKafkaSink: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editRabbitMqSink: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    query: (indexName?: string) => KnockoutComputed<string>;
    terms: (indexName?: string) => KnockoutComputed<string>;
    importDatabaseFromFileUrl: KnockoutComputed<string>;
    importCollectionFromCsv: KnockoutComputed<string>;
    importDatabaseFromSql: KnockoutComputed<string>;
    exportDatabaseUrl: KnockoutComputed<string>;
    migrateRavenDbDatabaseUrl: KnockoutComputed<string>;
    migrateDatabaseUrl: KnockoutComputed<string>;
    sampleDataUrl: KnockoutComputed<string>;
    backupsUrl: KnockoutComputed<string>;
    ongoingTasksUrl: KnockoutComputed<string>;
    editExternalReplicationTaskUrl: KnockoutComputed<string>;
    editReplicationHubTaskUrl: KnockoutComputed<string>;
    editReplicationSinkTaskUrl: KnockoutComputed<string>;
    editSubscriptionTaskUrl: KnockoutComputed<string>;
    editRavenEtlTaskUrl: KnockoutComputed<string>;
    editSqlEtlTaskUrl: KnockoutComputed<string>;
    editOlapEtlTaskUrl: KnockoutComputed<string>;
    editElasticSearchEtlTaskUrl: KnockoutComputed<string>;
    editKafkaEtlTaskUrl: KnockoutComputed<string>;
    editRabbitMqEtlTaskUrl: KnockoutComputed<string>;
    editKafkaSinkTaskUrl: KnockoutComputed<string>;
    editRabbitMqSinkTaskUrl: KnockoutComputed<string>;
    csvImportUrl: KnockoutComputed<string>;
    status: KnockoutComputed<string>;
    indexPerformance: KnockoutComputed<string>;
    indexCleanup: KnockoutComputed<string>;
    settings: KnockoutComputed<string>;
    indexErrors: KnockoutComputed<string>;
    ongoingTasksStats: KnockoutComputed<string>;
    runningQueries: KnockoutComputed<string>;
    visualizer: KnockoutComputed<string>;
    databaseSettings: KnockoutComputed<string>;
    databaseRecord: KnockoutComputed<string>;
    databaseIDs: KnockoutComputed<string>;
    tombstonesState: KnockoutComputed<string>;
    revisions: KnockoutComputed<string>;
    revertRevisions: KnockoutComputed<string>;
    expiration: KnockoutComputed<string>;
    dataArchival: KnockoutComputed<string>;
    documentsCompression: KnockoutComputed<string>;
    timeSeries: KnockoutComputed<string>;
    refresh: KnockoutComputed<string>;
    customSorters: KnockoutComputed<string>;
    customAnalyzers: KnockoutComputed<string>;
    editCustomAnalyzer: KnockoutComputed<string>;
    integrations: KnockoutComputed<string>;
    connectionStrings: KnockoutComputed<string>;
    conflictResolution: KnockoutComputed<string>;

    about: KnockoutComputed<string>;
    whatsNew: KnockoutComputed<string>;

    ioStats: KnockoutComputed<string>;

    statusStorageReport: KnockoutComputed<string>;
    statusBucketsReport: KnockoutComputed<string>;
    isAreaActive: (routeRoot: string) => KnockoutComputed<boolean>;
    isActive: (routeTitle: string) => KnockoutComputed<boolean>;
    databasesManagement: KnockoutComputed<string>;
}
