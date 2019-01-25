import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import deleteDocsMatchingQueryCommand = require("commands/database/documents/deleteDocsMatchingQueryCommand");
import database = require("models/resources/database");

class deleteDocumentsMatchingQueryConfirm extends dialogViewModelBase {
    constructor(private indexName: string, private queryText: string, private totalDocCount: number, private defaultOperator: string, private db: database) {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    deleteDocs() {
        new deleteDocsMatchingQueryCommand(this.indexName, this.queryText, this.defaultOperator, this.db).execute();
        dialog.close(this);
    }
}

export = deleteDocumentsMatchingQueryConfirm;
