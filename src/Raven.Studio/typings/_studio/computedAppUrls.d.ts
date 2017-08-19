// Interface
interface computedAppUrls {
    adminSettings: KnockoutComputed<string>;
    adminSettingsCluster: KnockoutComputed<string>;

    databases: KnockoutComputed<string>;
    manageDatabaseGroup: KnockoutComputed<string>;
    clientConfiguration: KnockoutComputed<string>;
    documents: KnockoutComputed<string>;
    revisionsBin: KnockoutComputed<string>;
    conflicts: KnockoutComputed<string>;
    patch: KnockoutComputed<string>;
    indexes: KnockoutComputed<string>;
    megeSuggestions: KnockoutComputed<string>;
    upgrade: KnockoutComputed<string>;
    newIndex: KnockoutComputed<string>;
    editIndex: (indexName?: string) => KnockoutComputed<string>;
    editExternalReplication: (taskId?: number) => KnockoutComputed<string>;
    editPeriodicBackupTask: (taskId?: number) => KnockoutComputed<string>;
    editSubscription: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    editRavenEtl: (taskId?: number, taskName?: string) => KnockoutComputed<string>;
    query: (indexName?: string) => KnockoutComputed<string>;
    terms: (indexName?: string) => KnockoutComputed<string>;
    reporting: KnockoutComputed<string>;
    exploration: KnockoutComputed<string>;
    tasks: KnockoutComputed<string>;
    importDatabaseUrl: KnockoutComputed<string>;
    exportDatabaseUrl: KnockoutComputed<string>;
    sampleDataUrl: KnockoutComputed<string>;
    ongoingTasksUrl: KnockoutComputed<string>;
    editExternalReplicationTaskUrl: KnockoutComputed<string>;
    editSubscriptionTaskUrl: KnockoutComputed<string>;
    editRavenEtlTaskUrl: KnockoutComputed<string>;
    csvImportUrl: KnockoutComputed<string>;
    status: KnockoutComputed<string>;
    indexPerformance: KnockoutComputed<string>;
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
    databaseRecord: KnockoutComputed<string>;
    quotas: KnockoutComputed<string>;
    revisions: KnockoutComputed<string>;
    connectionStrings: KnockoutComputed<string>;
    databaseStudioConfig: KnockoutComputed<string>;
    statusDebug: KnockoutComputed<string>;
    customFunctionsEditor: KnockoutComputed<string>;
    statusDebugChanges: KnockoutComputed<string>;
    statusDebugMetrics: KnockoutComputed<string>;
    statusDebugConfig: KnockoutComputed<string>;
    statusDebugDocrefs: KnockoutComputed<string>;
    statusDebugCurrentlyIndexing: KnockoutComputed<string>;
    statusDebugQueries: KnockoutComputed<string>;
    statusDebugTasks: KnockoutComputed<string>;
    statusDebugRoutes: KnockoutComputed<string>;
    statusDebugIndexFields: KnockoutComputed<string>;
    statusDebugIdentities: KnockoutComputed<string>;
    statusDebugWebSocket: KnockoutComputed<string>;
    infoPackage: KnockoutComputed<string>;

    about: KnockoutComputed<string>;

    ioStats: KnockoutComputed<string>;

    statusStorageReport: KnockoutComputed<string>;
    isAreaActive: (routeRoot: string) => KnockoutComputed<boolean>;
    isActive: (routeTitle: string) => KnockoutComputed<boolean>;
    databasesManagement: KnockoutComputed<string>;

    subscriptions: KnockoutComputed<string>;
}
