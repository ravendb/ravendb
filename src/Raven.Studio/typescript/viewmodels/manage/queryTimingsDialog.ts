import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import timingsChart from "common/timingsChart";

class queryTimingsDialog extends dialogViewModelBase {

    view = require("views/manage/queryTimingsDialog.html");

    timingsGraph = new timingsChart(".js-timings-container");
    
    private readonly query: string;
    
    private readonly timings: Raven.Client.Documents.Queries.Timings.QueryTimings;
    
    constructor(timings: Raven.Client.Documents.Queries.Timings.QueryTimings, query: string) {
        super(null);
        
        this.timings = timings;
        this.query = query;
    }

    executeQuery(query: string) {
        dialog.close(this, query);
    }
    
    compositionComplete(view?: any, parent?: any) {
        super.compositionComplete(view, parent);

        this.timingsGraph.draw(this.timings);
    }

    cancel() {
        dialog.close(this, false);
    }
}

export = queryTimingsDialog;
