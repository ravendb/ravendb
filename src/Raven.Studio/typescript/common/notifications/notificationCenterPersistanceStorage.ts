import resource = require("models/resources/resource");

class notificationCenterPersistanceStorage {

    private readonly serverStartTimeProvider: KnockoutObservable<string>;

    constructor(serverStartTimeProvider: KnockoutObservable<string>) {
        this.serverStartTimeProvider = serverStartTimeProvider;
    }

    loadOperations(rs: resource): Array<number> {
        const key = this.storageKey(rs);

        const operations = localStorage.getObject(key) as localStorageOperationsDto;

        if (!operations) {
            return [];
        }

        const startTime = this.serverStartTimeProvider();
        if (operations.ServerStartTime === startTime) {
            return operations.Operations;
        } else {
            this.removeSavedOperations(rs);
            return [];
        }
    }

    saveOperations(rs: resource, operations: Array<number>) {
        const startTime = this.serverStartTimeProvider();

        const dto: localStorageOperationsDto = {
            Operations: operations,
            ServerStartTime: startTime
        };

        const key = this.storageKey(rs);

        if (operations.length === 0) {
            localStorage.removeItem(key);
        } else {
            localStorage.setObject(key, dto);    
        }
    }

    //TODO: call when resource is deleted
    removeSavedOperations(rs: resource) {
        const key = this.storageKey(rs);
        localStorage.removeItem(key);
    }

    isStorageKeyMatch(rs: resource, storageEvent: StorageEvent) {
        return this.storageKey(rs) === storageEvent.key;
    }

    private storageKey(rs: resource) {
        return rs.fullTypeName + "_" + rs.name + "_operations" ;
    }
}

export = notificationCenterPersistanceStorage;