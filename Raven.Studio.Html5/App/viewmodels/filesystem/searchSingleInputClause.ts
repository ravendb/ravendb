import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class searchSingleInputClause extends dialogViewModelBase {

    public applyFilterTask = $.Deferred();
    filterText = ko.observable("");
    label = ko.observable("");

    constructor(label: string) {
        super();

        this.label(label);
    }

    cancel() {
        dialog.close(this);
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.filterText());
        dialog.close(this);
    }
}

export = searchSingleInputClause;