import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

//models
import replicationsSetup = require("models/replicationsSetup");
import replicationDestination = require("models/replicationDestination");

//viewmodels
import viewModelBase = require("viewmodels/viewModelBase");

//commands



class synchronizationDestinations extends viewModelBase {

    destinations = ko.observableArray<synchronizationDestinationDto>();
    isSetupSaveEnabled: KnockoutComputed<boolean>;
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var fs = this.activeFilesystem();
        if (fs) {
            this.fetchDestinations(fs)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forIndexes(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);

        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations()]);
        this.isSetupSaveEnabled = ko.computed(()=> this.replicationsSetupDirtyFlag().isDirty());
        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.replicationsSetupDirtyFlag]);
    }

    saveChanges() {
        
    }

    createNewDestination() {
        this.destinations.unshift(replicationDestination.empty());
    }

    fetchDestinations(fs): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getReplicationsCommand(fs)
            .execute()
            .done(repSetup => this.replicationsSetup(new replicationsSetup(repSetup)))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

}

export = synchronizationDestinations;