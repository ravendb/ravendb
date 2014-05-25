import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import counterStorage = require("models/counter/counterStorage");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");
import createCounterStorageCommand = require("commands/counter/createCounterStorageCommand");
import appUrl = require("common/appUrl");

class counterStorages extends viewModelBase {

    storages = ko.observableArray<counterStorage>();
    hasCounterStorages = ko.computed(() => this.storages().length >0 );
    isFirstLoad: boolean = true;
    selectedCounterStorage = ko.observable<counterStorage>();
    searchCounterStorageByText = ko.observable<string>();


    constructor() {
        super();
        this.searchCounterStorageByText.extend({ throttle: 200 }).subscribe(s => this.filterCounterStorages(s));
    }

    canActivate(args: any): any {
        return true;
        
    }

    filterCounterStorages(filterString: string) {
        var filterStringLower = filterString.toLowerCase();
        this.storages().forEach(x => {
            var isMatch = !filterString|| (x.name.toLowerCase().indexOf(filterStringLower) >= 0);
            x.isVisible(isMatch);
        });

        var selectedCounterStorage = this.selectedCounterStorage();
        if (selectedCounterStorage && !selectedCounterStorage.isVisible()) {
            selectedCounterStorage.isSelected(false);
            this.selectedCounterStorage(null);
        }
    }
    
    deleteSelectedCounterStorage() {
        var counterStorage = this.selectedCounterStorage();
        if (!!counterStorage) {
            require(["viewmodels/counter/deleteCounterStorageConfirm"], deleteCounterStorageConfirm => {
                var confirmDeleteVm = new deleteCounterStorageConfirm(counterStorage);
                confirmDeleteVm.deleteTask.done(() => {
                    this.onCounterStorageDeleted(counterStorage);
                    this.selectedCounterStorage(null);
                });
                app.showDialog(confirmDeleteVm);
            });
        }
    }

    onCounterStorageDeleted(storage: counterStorage) {
        this.storages.remove(storage);
        if (this.storages.length > 0 && this.storages.contains(this.selectedCounterStorage()) === false)
            this.selectCounterStorage(this.storages().first());
        
    }

    createNewCountersStorage() {
        require(["viewmodels/counter/createCounterStorage"], createCounterStorage => {
            var createCounterStorageiewModel = new createCounterStorage(this.storages);
            createCounterStorageiewModel
                .creationTask
                .done((counterStorageName: string, counterStoragePath: string) => {
                    counterStoragePath = !!counterStoragePath && counterStoragePath.length > 0 ? counterStoragePath : "~/Counters/" + counterStorageName;
                    this.showCreationAdvancedStepsIfNecessary(counterStorageName, counterStoragePath);
            });
            app.showDialog(createCounterStorageiewModel);
        });
    }

    showCreationAdvancedStepsIfNecessary(counterStorageName: string, counterStoragePath: string) {
        new createCounterStorageCommand(counterStorageName, counterStoragePath)
            .execute()
            .done(() => this.storages.unshift(new counterStorage(counterStorageName)));
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