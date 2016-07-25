import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import counterStorage = require("models/counter/counterStorage");
import counterStorageReplicationSetup = require("models/counter/counterStorageReplicationSetup");
import counterStorageReplicationDestination = require("models/counter/counterStorageReplicationDestination");
import getCounterStorageReplicationCommand = require("commands/counter/getCounterStorageReplicationCommand");
import saveCounterStorageReplicationCommand = require("commands/counter/saveCounterStorageReplicationCommand");

class counterStorageReplication extends viewModelBase {
    appUrls: computedAppUrls;
    replicationsSetup = ko.observable<counterStorageReplicationSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentCounterStorage();
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var counterStorage = this.activeCounterStorage();
        if (counterStorage) {
            this.fetchCountersDestinations(counterStorage)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forCounterStorage(counterStorage) }));
        }
        return deferred;
    }

    activate(args: any) {
        super.activate(args);

        this.dirtyFlag = new ko.DirtyFlag([this.replicationsSetup]);
        this.isSaveEnabled = ko.computed(() => {
            return this.dirtyFlag().isDirty();
        });
    }

    fetchCountersDestinations(counterStorage: counterStorage, reportFetchProgress: boolean = false): JQueryPromise<any> {
        var deferred = $.Deferred();
        if (counterStorage) {
            new getCounterStorageReplicationCommand(counterStorage, reportFetchProgress)
                .execute()
                .done(data => this.replicationsSetup(new counterStorageReplicationSetup({ Destinations: null /* TODO: data.Destinations - looks like it isn't available on server side for now */ })))
                .fail(() => this.replicationsSetup(new counterStorageReplicationSetup({ Destinations: [] })))
                .always(() => deferred.resolve({ can: true }));
        }
        return deferred;
    }

    saveChanges() {
        var counter = this.activeCounterStorage();
        if (counter) {
            new saveCounterStorageReplicationCommand(this.replicationsSetup().toDto(), counter)
                .execute()
                .done(() => this.dirtyFlag().reset());
        }
    }

    createNewDestination() {
        var cs = this.activeCounterStorage();
        this.replicationsSetup().destinations.unshift(counterStorageReplicationDestination.empty(cs.name));
    }

    removeDestination(resplicationDestination: counterStorageReplicationDestination) {
        this.replicationsSetup().destinations.remove(resplicationDestination);
    }

    refreshFromServer() {
        var canContinue = this.canContinueIfNotDirty('Unsaved Data', 'You have unsaved data. Are you sure you want to refresh the data from the server?');
        canContinue.done(() => {
            this.fetchCountersDestinations(this.activeCounterStorage(), true)
                .done(() => this.dirtyFlag().reset());
        });
    }
}

export = counterStorageReplication;
