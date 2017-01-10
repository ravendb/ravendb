import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class detailsItem {
    key: string;
    value: string;
    indent: number;
}

class indexPerformanceDetails extends dialogViewModelBase {

    element: Raven.Client.Data.Indexes.IndexingPerformanceOperation;

    constructor(element: Raven.Client.Data.Indexes.IndexingPerformanceOperation) {
        super();
        this.element = element;
    }

    close() {
        dialog.close(this);
    }
}

export = indexPerformanceDetails; 
 
