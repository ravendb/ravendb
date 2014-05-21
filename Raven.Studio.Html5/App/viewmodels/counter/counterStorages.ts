import viewModelBase = require("viewmodels/viewModelBase");
import counterStorage = require("models/counter/counterStorage");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");
import appUrl = require("common/appUrl");

class counterStorages extends viewModelBase {

    storages = ko.observableArray<counterStorage>();
    hasCounterStorages = ko.computed(() => this.storages().length >0 );
    isFirstLoad: boolean = true;
    selectedCounterStorage = ko.observable<counterStorage>();

    canActivate(args: any): any {
        return true;
    }

    createNewCountersStorage() {
        //todo: implement creation of new counter storage
    }

    countersStoragesLoaded(storages: counterStorage[]) {
        if (storages.length === 0 && this.isFirstLoad) {
            this.createNewCountersStorage();
        }

        var counterSotragesHaveChanged = this.checkDifferentCounterStorages(storages);
        if (counterSotragesHaveChanged) {
            this.storages(storages);
        }

        this.isFirstLoad = false;
    }

    checkDifferentCounterStorages(storages: counterStorage[]) {
        if (storages.length !== this.storages().length) {
            return true;
        }

        var freshStorageNames = storages.map(storage => storage.name);
        var existingStorageNames = this.storages().map(d => d.name);
        return existingStorageNames.some(existing => !freshStorageNames.contains(existing));
    }

    modelPolling() {
        new getCounterStoragesCommand().execute().done((results: counterStorage[]) => this.countersStoragesLoaded(results));
    }

    selectCounterStorage(storage: counterStorage) {
        this.storages().forEach(d=> d.isSelected(d == storage));
        storage.activate();
        this.selectedCounterStorage(storage);
    }

    getCounterStorageUrl(storage: counterStorage) {
        return appUrl.forCounterStorageCounters(storage);
    }

    
}

export = counterStorages;