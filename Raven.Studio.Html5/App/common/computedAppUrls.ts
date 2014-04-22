// Interface
interface computedAppUrls {
    documents: KnockoutComputed<string>;
    conflicts: KnockoutComputed<string>;
    patch: KnockoutComputed<string>;
    indexes: KnockoutComputed<string>;
    newIndex: KnockoutComputed<string>;
    editIndex: (indexName?: string) => KnockoutComputed<string>;
    newTransformer: KnockoutComputed<string>;
    editTransformer: (transformerName?: string) => KnockoutComputed<string>;
    transformers: KnockoutComputed<string>;
    query: (indexName?: string) => KnockoutComputed<string>;
    reporting: KnockoutComputed<string>;
    tasks: KnockoutComputed<string>;
    status: KnockoutComputed<string>;
    settings: KnockoutComputed<string>;
    logs: KnockoutComputed<string>;
    alerts: KnockoutComputed<string>;
    indexErrors: KnockoutComputed<string>;
    replicationStats: KnockoutComputed<string>;
    userInfo: KnockoutComputed<string>;
    databaseSettings: KnockoutComputed<string>;
    periodicBackup: KnockoutComputed<string>;
    replications: KnockoutComputed<string>;
    sqlReplications: KnockoutComputed<string>;
    scriptedIndexes: KnockoutComputed<string>;
    statusDebug: KnockoutComputed<string>;
    statusDebugChanges: KnockoutComputed<string>;
    statusDebugMetrics: KnockoutComputed<string>;
    statusDebugConfig: KnockoutComputed<string>;
    statusDebugDocrefs: KnockoutComputed<string>;
    statusDebugCurrentlyIndexing: KnockoutComputed<string>;
    statusDebugQueries: KnockoutComputed<string>;
    statusDebugTasks: KnockoutComputed<string>;


    isActive: (routeTitle: string) => KnockoutComputed<boolean>;
    databasesManagement: KnockoutComputed<string>;
}