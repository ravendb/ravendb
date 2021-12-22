import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");

class deleteDocumentsMatchingQueryConfirm extends dialogViewModelBase {

    view = require("views/database/query/deleteDocumentsMatchingQueryConfirm.html");
    
    constructor(private indexName: string, private queryText: string, private totalDocCount: number, private db: database, private hasMore: boolean = false) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    deleteDocs() {
        dialog.close(this, true);
    }
}

export = deleteDocumentsMatchingQueryConfirm;
