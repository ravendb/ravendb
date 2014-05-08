import getStatusDebugQueriesCommand = require("commands/getStatusDebugQueriesCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import statusDebugQueriesGroup = require("models/statusDebugQueriesGroup");


class statusDebugQueries extends viewModelBase {
    data = ko.observable<statusDebugQueriesGroup[]>();
    autoRefresh = ko.observable<boolean>(true);

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchCurrentQueries());
        return this.fetchCurrentQueries();
    }

    modelPolling() {
        if (this.autoRefresh()) {
            this.fetchCurrentQueries();
        }
    }

    toggleAutoRefresh() {
        this.autoRefresh(!this.autoRefresh());
        $("#refresh-btn").blur();
    }

    fetchCurrentQueries(): JQueryPromise<statusDebugQueriesGroup[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugQueriesCommand(db)
                .execute()
                .done((results: statusDebugQueriesGroup[]) => this.data(results));
        }

        return null;
    }
}

export = statusDebugQueries;