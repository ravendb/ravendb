import stopIndexingCommand = require("commands/stopIndexingCommand");
import startIndexingCommand = require("commands/startIndexingCommand");
import getIndexingStatusCommand = require("commands/getIndexingStatusCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class toggleIndexing extends viewModelBase {

    indexingStatus = ko.observable<string>();
    isIndexingEnabled: KnockoutComputed<boolean>;
    isIndexingDisabled: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.isIndexingEnabled = ko.computed(() => !!this.indexingStatus() && this.indexingStatus() !== "Paused");
        this.isIndexingDisabled = ko.computed(() => !!this.indexingStatus() && this.indexingStatus() !== "Indexing" && this.indexingStatus() !== "Started");
    }

    canActivate(args): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        this.getIndexStatus()
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forTasks(this.activeDatabase()) }));
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("VXOPAN");
    }

    disableIndexing() {
        new stopIndexingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    enableIndexing() {
        new startIndexingCommand(this.activeDatabase())
            .execute()
            .done(() => this.getIndexStatus());
    }

    getIndexStatus() {
        var deferred = $.Deferred();

        new getIndexingStatusCommand(this.activeDatabase())
            .execute()
            .done(result => {
                this.indexingStatus(result.IndexingStatus);
                deferred.resolve();
            })
            .fail(() => deferred.reject());

        return deferred;
    }

}

export = toggleIndexing;
