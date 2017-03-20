import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class queryStatsDialog extends dialogViewModelBase {

    //TODO: detect index change (take code from v3.5)
    constructor(private queryStats: Raven.Client.Documents.Queries.QueryResult<any>, private indexUsedForQuery: string) {
        super();
    }
    
    cancel() {                
        dialog.close(this);
    }

}

export = queryStatsDialog;
