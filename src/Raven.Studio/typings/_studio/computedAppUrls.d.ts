// Interface
interface computedAppUrls {
    adminSettings: KnockoutComputed<string>;
    adminSettingsCluster: KnockoutComputed<string>;

    hasApiKey: KnockoutComputed<string>;

    resources: KnockoutComputed<string>;
    documents: KnockoutComputed<string>;
    conflicts: KnockoutComputed<string>;
    patch: KnockoutComputed<string>;
    indexes: KnockoutComputed<string>;
    megeSuggestions: KnockoutComputed<string>;
    upgrade: KnockoutComputed<string>;
    newIndex: KnockoutComputed<string>;
    editIndex: (indexName?: string) => KnockoutComputed<string>;
    newTransformer: KnockoutComputed<string>;
    editTransformer: (transformerName?: string) => KnockoutComputed<string>;
    transformers: KnockoutComputed<string>;
    query: (indexName?: string) => KnockoutComputed<string>;
    reporting: KnockoutComputed<string>;
    exploration: KnockoutComputed<string>;
    tasks: KnockoutComputed<string>;
    status: KnockoutComputed<string>;
    indexPerformance: KnockoutComputed<string>;
    indexStats: KnockoutComputed<string>;
    indexBatchSize: KnockoutComputed<string>;
    indexPrefetches: KnockoutComputed<string>;
    replicationPerfStats: KnockoutComputed<string>;
    sqlReplicationPerfStats: KnockoutComputed<string>;
    requestsCount: KnockoutComputed<string>;
    requestsTracing: KnockoutComputed<string>;
    settings: KnockoutComputed<string>;
    logs: KnockoutComputed<string>;
    runningTasks: KnockoutComputed<string>;
    alerts: KnockoutComputed<string>;
    indexErrors: KnockoutComputed<string>;
    replicationStats: KnockoutComputed<string>;
    userInfo: KnockoutComputed<string>;
    visualizer: KnockoutComputed<string>;
    databaseSettings: KnockoutComputed<string>;
    quotas: KnockoutComputed<string>;
    periodicExport: KnockoutComputed<string>;
    replications: KnockoutComputed<string>;
    etl: KnockoutComputed<string>;
    hotSpare: KnockoutComputed<string>;
    versioning: KnockoutComputed<string>;
    sqlReplications: KnockoutComputed<string>;
    sqlReplicationsConnections: KnockoutComputed<string>;
    editSqlReplication: KnockoutComputed<string>;
    scriptedIndexes: KnockoutComputed<string>;
    customFunctionsEditor: KnockoutComputed<string>;
    databaseStudioConfig: KnockoutComputed<string>;
    statusDebug: KnockoutComputed<string>;
    statusDebugChanges: KnockoutComputed<string>;
    statusDebugMetrics: KnockoutComputed<string>;
    statusDebugConfig: KnockoutComputed<string>;
    statusDebugDocrefs: KnockoutComputed<string>;
    statusDebugCurrentlyIndexing: KnockoutComputed<string>;
    statusDebugQueries: KnockoutComputed<string>;
    statusDebugTasks: KnockoutComputed<string>;
    statusDebugRoutes: KnockoutComputed<string>;
    statusDebugSqlReplication: KnockoutComputed<string>;
    statusDebugIndexFields: KnockoutComputed<string>;
    statusDebugIdentities: KnockoutComputed<string>;
    statusDebugWebSocket: KnockoutComputed<string>;
    statusDebugExplainReplication: KnockoutComputed<string>;
    infoPackage: KnockoutComputed<string>;

    ioStats: KnockoutComputed<string>;

    statusStorageOnDisk: KnockoutComputed<string>;
    statusStorageBreakdown: KnockoutComputed<string>;
    statusStorageCollections: KnockoutComputed<string>;

    isAreaActive: (routeRoot: string) => KnockoutComputed<boolean>;
    isActive: (routeTitle: string) => KnockoutComputed<boolean>;
    resourcesManagement: KnockoutComputed<string>;

    filesystemFiles: KnockoutComputed<string>;
    filesystemSearch: KnockoutComputed<string>;
    filesystemSynchronization: KnockoutComputed<string>;
    filesystemStatus: KnockoutComputed<string>;
    filesystemTasks: KnockoutComputed<string>;
    filesystemSettings: KnockoutComputed<string>;
    filesystemSynchronizationDestinations: KnockoutComputed<string>;
    filesystemConfiguration: KnockoutComputed<string>;
    filesystemSynchronizationConfiguration: KnockoutComputed<string>;
    filesystemVersioning: KnockoutComputed<string>;

    counterStorages:KnockoutComputed<string>;
    counterStorageCounters: KnockoutComputed<string>;
    counterStorageReplication: KnockoutComputed<string>;
    counterStorageTasks: KnockoutComputed<string>;
    counterStorageStats: KnockoutComputed<string>;
    counterStorageConfiguration: KnockoutComputed<string>;

    timeSeriesType: KnockoutComputed<string>;
    timeSeriesPoints: KnockoutComputed<string>;
    timeSeriesStats: KnockoutComputed<string>;
    timeSeriesConfiguration: KnockoutComputed<string>;
    timeSeriesConfigurationTypes: KnockoutComputed<string>;

    dataSubscriptions: KnockoutComputed<string>;
}
