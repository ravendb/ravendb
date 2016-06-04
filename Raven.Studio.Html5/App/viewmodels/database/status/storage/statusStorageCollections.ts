import getSlowDocCountsCommand = require("commands/database/debug/getSlowDocCountsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import debugDocumentStats = require("models/database/debug/debugDocumentStats");
import genUtils = require("common/generalUtils");

class statusStorageCollections extends viewModelBase {
    data = ko.observable<debugDocumentStats>();
    canSearch = ko.observable(true);

    formatTimeSpan = genUtils.formatTimeSpan;
    formatBytesToSize = genUtils.formatBytesToSize;

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
            return new getSlowDocCountsCommand(db)
                .execute()
                .done((results: debugDocumentStats) => {
                    this.data(results);
                })
                .always(() => this.canSearch(true));
        }

        return null;
    }
}

export = statusStorageCollections;
