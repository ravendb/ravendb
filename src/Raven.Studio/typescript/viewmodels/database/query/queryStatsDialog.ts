import appUrl = require("common/appUrl");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class queryStatsDialog extends dialogViewModelBase {

    public static readonly AllDocs = "AllDocs";

    selectedIndexEditUrl: string;

    canNavigateToIndex: boolean;

    constructor(private queryStats: Raven.Client.Documents.Queries.QueryResult<any, any>, private db: database) {
        super();

        this.selectedIndexEditUrl = appUrl.forEditIndex(queryStats.IndexName, this.db);

        this.canNavigateToIndex = this.physicalIndexExists(queryStats.IndexName);
    }

    private physicalIndexExists(indexName: string) {
        if (!indexName || indexName === queryStatsDialog.AllDocs) {
            return false;
        }
        if (indexName.startsWith("collection/")) {
            return false;
        }
        return true;
    }
    
    cancel() {                
        dialog.close(this);
    }

}

export = queryStatsDialog;
