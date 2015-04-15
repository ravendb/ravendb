import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import shell = require('viewmodels/shell');
import counterStorage = require("models/counter/counterStorage");
import createCounterStorageCommand = require("commands/counter/createCounterStorageCommand");
import appUrl = require("common/appUrl");
import createCounterStorage = require("viewmodels/counter/createCounterStorage");
import deleteCounterStorageConfirm = require("viewmodels/counter/deleteCounterStorageConfirm");

class counterStorages extends viewModelBase {

    counterStorages = ko.observableArray<counterStorage>();
    selectedCounterStorage = ko.observable<counterStorage>();
    searchCounterStorageByText = ko.observable<string>();
    optionsClicked = ko.observable<boolean>(false);

    constructor() {
        super();

        this.counterStorages = shell.counterStorages;
        this.searchCounterStorageByText.extend({ throttle: 200 }).subscribe(s => this.filterCounterStorages(s));

        var currentCounterStorage = this.activeCounterStorage();
        if (!!currentCounterStorage) {
            this.selectCounterStorage(currentCounterStorage, false);
        }
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    attached() {
        this.counterStoragesLoaded();
    }

    private counterStoragesLoaded() {
        // If we have no counter storages, show the "create a new counter storage" screen.
        if (this.counterStorages().length === 0) {
            this.createNewCountersStorage();
        } else {
            // If we have just a few counter storages, grab the cs stats for all of them.
            // (Otherwise, we'll grab them when we click them.)
            var few = 20;
            var enabledCounterStorages: counterStorage[] = this.counterStorages().filter((db: counterStorage) => !db.disabled());
            if (enabledCounterStorages.length < few) {
                enabledCounterStorages.forEach(cs => shell.fetchCsStats(cs));
            }
        }
    }

    private addNewCounterStorage(counterStorageName: string): counterStorage {
        var counterStorageInArray = this.counterStorages.first((cs: counterStorage) => cs.name == counterStorageName);

        if (!counterStorageInArray) {
            var newCounterStorage = new counterStorage(counterStorageName);
            this.counterStorages.unshift(newCounterStorage);
            return newCounterStorage;
        }

        return counterStorageInArray;
    }

    private onCounterStorageDeleted(counterStorageName: string) {
        var counterStorageInArray = this.counterStorages.first((cs: counterStorage) => cs.name == counterStorageName);

        if (!!counterStorageInArray) {
            this.counterStorages.remove(counterStorageInArray);

            if ((this.counterStorages().length > 0) && (this.counterStorages.contains(this.activeCounterStorage()) === false)) {
                this.selectCounterStorage(this.counterStorages().first());
            }
        }
    }

    private filterCounterStorages(filterString: string) {
        var filterStringLower = filterString.toLowerCase();
        this.counterStorages().forEach(x => {
            var isMatch = !filterString|| (x.name.toLowerCase().indexOf(filterStringLower) >= 0);
            x.isVisible(isMatch);
        });

        var selectedCounterStorage = this.selectedCounterStorage();
        if (selectedCounterStorage && !selectedCounterStorage.isVisible()) {
            selectedCounterStorage.isSelected(false);
            this.selectedCounterStorage(null);
        }
    }
    
    createNewCountersStorage() {
        var createCounterStorageiewModel = new createCounterStorage(this.counterStorages);
        createCounterStorageiewModel
            .creationTask
            .done((counterStorageName: string, counterStoragePath: string) => {
                counterStoragePath = !!counterStoragePath && counterStoragePath.length > 0 ? counterStoragePath : "~/Counters/" + counterStorageName;
                this.showCreationAdvancedStepsIfNecessary(counterStorageName, counterStoragePath);
            });
        app.showDialog(createCounterStorageiewModel);
    }

    private showCreationAdvancedStepsIfNecessary(counterStorageName: string, counterStoragePath: string) {
        new createCounterStorageCommand(counterStorageName, counterStoragePath)
            .execute()
            .done(() => {
                var newCounterStorage = this.addNewCounterStorage(counterStorageName);
                this.selectCounterStorage(newCounterStorage);
            });
    }

    deleteSelectedCounterStorage() {
        var cs: counterStorage = this.selectedCounterStorage();
        if (!!cs) {
            var confirmDeleteVm = new deleteCounterStorageConfirm(cs);
            confirmDeleteVm.deleteTask.done(() => this.onCounterStorageDeleted(cs.name));
            app.showDialog(confirmDeleteVm);
        }
    }

    selectCounterStorage(cs: counterStorage, activateCounterStorage: boolean = true) {
        if (this.optionsClicked() == false) {
            if (activateCounterStorage) {
                cs.activate();
            }
            this.selectedCounterStorage(cs);
        }

        this.optionsClicked(false);
    }

    getCounterStorageUrl(storage: counterStorage) {
        return appUrl.forCounterStorageCounters(storage);
    }
}

export = counterStorages;