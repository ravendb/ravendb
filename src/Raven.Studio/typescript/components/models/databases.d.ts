import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;

export interface DatabaseSharedInfo {
    name: string;
    sharded: boolean;
    lockMode: DatabaseLockMode;
    encrypted: boolean;
}


export interface DatabaseFilterCriteria {
    searchText: string;
}
