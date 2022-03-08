import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class trafficWatchQueriesDialog extends dialogViewModelBase {

    view = require("views/manage/trafficWatchQueriesDialog.html");
    
    constructor(private queryList: string[]) {
        super(null);

        if (queryList.length === 0) {
            console.warn("Must have at least one query for this dialog.");
        }
    }

    executeQuery(query: string) {
        dialog.close(this, query);
    }
    
    cancel() {
        dialog.close(this, false);
    }
}

export = trafficWatchQueriesDialog;
