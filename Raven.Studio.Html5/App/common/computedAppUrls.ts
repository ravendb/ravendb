// Interface
interface computedAppUrls {
    databases: KnockoutComputed<string>;
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

    isAreaActive: (routeRoot: string) => KnockoutComputed<boolean>;
    isActive: (routeTitle: string) => KnockoutComputed<boolean>;
    databasesManagement: KnockoutComputed<string>;
    filesystemsManagement: KnockoutComputed<string>;

    filesystems: KnockoutComputed<string>;
    filesystemFiles: KnockoutComputed<string>;
    filesystemSearch: KnockoutComputed<string>;
    filesystemSynchronization: KnockoutComputed<string>;
    filesystemConfiguration: KnockoutComputed<string>;
}