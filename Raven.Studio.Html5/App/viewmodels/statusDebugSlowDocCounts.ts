import getStatusDebugSlowDocCountsCommand = require("commands/getStatusDebugSlowDocCountsCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import debugDocumentStats = require("models/debugDocumentStats");


class statusDebugSlowDocCounts extends viewModelBase {
    data = ko.observable<debugDocumentStats>();
    canSearch = ko.observable(true);


    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.resetView());
        return this.resetView();
    }

    resetView() {
        this.data(null);
        this.canSearch(true);
    }

    fetchDocCounts(): JQueryPromise<debugDocumentStats> {
        var db = this.activeDatabase();
        if (db) {
            this.canSearch(false);
            return new getStatusDebugSlowDocCountsCommand(db)
                .execute()
                .done((results: debugDocumentStats) => this.data(results))
                .always(() => this.canSearch(true));

        }

        return null;
    }
}

export = statusDebugSlowDocCounts;