import getStatusDebugCurrentlyIndexingCommand = require("commands/database/debug/getStatusDebugCurrentlyIndexingCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class statusDebugCurrentlyIndexing extends viewModelBase {
    data = ko.observable<statusDebugCurrentlyIndexingDto>();
    autoRefresh = ko.observable<boolean>(true);

    activate(args) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.activeDatabase.subscribe(() => this.fetchCurrentlyIndexing());
        return this.fetchCurrentlyIndexing();
    }

    modelPolling() {
        if (this.autoRefresh()) {
            return this.fetchCurrentlyIndexing();
        }
        return $.Deferred().resolve();
    }

    toggleAutoRefresh() {
        this.autoRefresh(!this.autoRefresh());
        $("#refresh-btn").blur();
    }

    fetchCurrentlyIndexing(): JQueryPromise<statusDebugCurrentlyIndexingDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugCurrentlyIndexingCommand(db)
                .execute()
                .done((results: statusDebugCurrentlyIndexingDto) => this.data(results));
        }

        return null;
    }
}

export = statusDebugCurrentlyIndexing;