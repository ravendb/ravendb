import getStatusDebugCurrentlyIndexingCommand = require("commands/getStatusDebugCurrentlyIndexingCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import changeSubscription = require("models/changeSubscription");
import shell = require("viewmodels/shell");


class statusDebugCurrentlyIndexing extends viewModelBase {
    data = ko.observable<statusDebugCurrentlyIndexingDto>();
    autoRefresh = ko.observable<boolean>(true);

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchCurrentlyIndexing());
        return this.fetchCurrentlyIndexing();
    }

    modelPolling() {
        if (this.autoRefresh()) {
            this.fetchCurrentlyIndexing();
        }
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