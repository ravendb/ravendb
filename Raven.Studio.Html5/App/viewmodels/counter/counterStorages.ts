import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import shell = require('viewmodels/shell');
import counterStorage = require("models/counter/counterStorage");
import changeSubscription = require("models/changeSubscription");
import getCounterStoragesCommand = require("commands/counter/getCounterStoragesCommand");
import createCounterStorageCommand = require("commands/counter/createCounterStorageCommand");
import appUrl = require("common/appUrl");

class counterStorages extends viewModelBase {

    counterStorages = ko.observableArray<counterStorage>();
    selectedCounterStorage = ko.observable<counterStorage>();
    searchCounterStorageByText = ko.observable<string>();

    constructor() {
        super();
        this.searchCounterStorageByText.extend({ throttle: 200 }).subscribe(s => this.filterCounterStorages(s));
    }

    canActivate(args: any): any {
        var deferred = $.Deferred();

        this.fetchCounterStorages()
            .done(() => deferred.resolve({ can: true }));

        return deferred;
    }

    activate(args) {
        super.activate(args);

        if (this.counterStorages().length == 0) {
            this.createNewCountersStorage();
        }
    }

    private fetchCounterStorages(): JQueryPromise<any> {
        return new getCounterStoragesCommand()
            .execute()
            .done((results: counterStorage[]) => this.counterStorages(results));
    }

    createNotifications(): Array<changeSubscription> {
        return [
            shell.globalChangesApi.watchDocsStartingWith("Raven/Counters/", (e) => this.changesApiFiredForCounterStorages(e))
        ];
    }

    private changesApiFiredForCounterStorages(e: documentChangeNotificationDto) {
        if (!!e.Id && (e.Type === documentChangeType.Delete || e.Type === documentChangeType.Put)) {
            var receivedCounterStoragesName = e.Id.slice(e.Id.lastIndexOf('/') + 1);

            if (e.Type === documentChangeType.Delete) {
                this.onCounterStorageDeleted(receivedCounterStoragesName);
            } else {
                var existingCounterStorage = this.counterStorages.first((cs: counterStorage) => cs.name == receivedCounterStoragesName);
                var receivedCounterStorageDisabled: boolean = (e.Type === documentChangeType.Put);

                if (existingCounterStorage == null) {
                    this.addNewCounterStorage(receivedCounterStoragesName, receivedCounterStorageDisabled);
                }
                else if (existingCounterStorage.disabled() != receivedCounterStorageDisabled) {
                    existingCounterStorage.disabled(receivedCounterStorageDisabled);
                }
            }
        }
    }

    private onCounterStorageDeleted(counterStorageName: string) {
        var counterStoragesInList = this.counterStorages.first((cs: counterStorage) => cs.name == counterStorageName);
        if (!!counterStoragesInList) {
            this.counterStorages.remove(counterStoragesInList);

            if ((this.counterStorages().length > 0) && (this.counterStorages.contains(this.activeCounterStorage()) === false)) {
                this.selectCounterStorage(this.counterStorages().first());
            }
        }
    }

    private addNewCounterStorage(counterStorageName: string, isCounterStorageDisabled: boolean = false): counterStorage {
        var counterStorageInList = this.counterStorages.first((cs: counterStorage) => cs.name == counterStorageName);

        if (!counterStorageInList) {
            var newCounterStorage = new counterStorage(counterStorageName, isCounterStorageDisabled);
            this.counterStorages.unshift(newCounterStorage);
            return newCounterStorage;
        }

        return counterStorageInList;
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
        require(["viewmodels/counter/createCounterStorage"], createCounterStorage => {
            var createCounterStorageiewModel = new createCounterStorage(this.counterStorages);
            createCounterStorageiewModel
                .creationTask
                .done((counterStorageName: string, counterStoragePath: string) => {
                    counterStoragePath = !!counterStoragePath && counterStoragePath.length > 0 ? counterStoragePath : "~/Counters/" + counterStorageName;
                    this.showCreationAdvancedStepsIfNecessary(counterStorageName, counterStoragePath);
                });
            app.showDialog(createCounterStorageiewModel);
        });
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
            require(["viewmodels/counter/deleteCounterStorageConfirm"], deleteCounterStorageConfirm => {
                var confirmDeleteVm = new deleteCounterStorageConfirm(cs);
                confirmDeleteVm.deleteTask.done(() => this.onCounterStorageDeleted(cs.name));
                app.showDialog(confirmDeleteVm);
            });
        }
    }

    selectCounterStorage(storage: counterStorage) {
        this.counterStorages().forEach(d=> d.isSelected(d == storage));
        storage.activate();
        this.selectedCounterStorage(storage);
    }

    getCounterStorageUrl(storage: counterStorage) {
        return appUrl.forCounterStorageCounters(storage);
    }
}

export = counterStorages;