import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import counterStorageReplicationSetup = require("models/counter/counterStorageReplicationSetup");
import counterStorageReplicationDestination = require("models/counter/counterStorageReplicationDestination");
import getStorageCounterDestinationsCommand = require("commands/counter/getCounterStorageReplicationCommand");
import saveStorageCounterDestinationCommand = require("commands/counter/saveCounterStorageReplicationCommand");

class counterStorageReplication extends viewModelBase {

    replicationsSetup = ko.observable<counterStorageReplicationSetup>().extend({ required: true });
    isSaveEnabled: KnockoutComputed<boolean>;

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

    activate(args) {
        super.activate(args);

        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.replicationsSetup]);
        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
    }

    fetchCountersDestinations(counterStorage): JQueryPromise<any> {
        var deferred = $.Deferred();
        if (counterStorage) {
            new getStorageCounterDestinationsCommand(counterStorage)
                .execute()
                .done(data => this.replicationsSetup(new counterStorageReplicationSetup({ Destinations: data.Destinations })))
                .fail(() => this.replicationsSetup(new counterStorageReplicationSetup({ Destinations: [] })))
                .always(() => deferred.resolve({ can: true }));
        }
        return deferred;
    }

    saveChanges() {
        var counter = this.activeCounterStorage();
        if (counter) {
            new saveStorageCounterDestinationCommand(this.replicationsSetup().toDto(), counter)
                .execute()
                .done(()=> viewModelBase.dirtyFlag().reset());
        }
    }

    createNewDestination() {
        this.replicationsSetup().destinations.unshift(counterStorageReplicationDestination.empty());
    }

    removeDestination(resplicationDestination: counterStorageReplicationDestination) {
        this.replicationsSetup().destinations.remove(resplicationDestination);
    }
}

export = counterStorageReplication;