// Interface
interface computedAppUrls {
    documents: KnockoutComputed<string>;
    indexes: KnockoutComputed<string>;
    newIndex: KnockoutComputed<string>;
    editIndex: (indexName: string) => KnockoutComputed<string>;
    query: KnockoutComputed<string>;
    status: KnockoutComputed<string>;
    settings: KnockoutComputed<string>;
    logs: KnockoutComputed<string>;
    alerts: KnockoutComputed<string>;
    indexErrors: KnockoutComputed<string>;
    replicationStats: KnockoutComputed<string>;
    userInfo: KnockoutComputed<string>;
    databaseSettings: KnockoutComputed<string>;
    periodicBackup: KnockoutComputed<string>;
}