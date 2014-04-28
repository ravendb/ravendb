import searchDialogViewModel = require("viewmodels/filesystem/searchDialogViewModel");
import dialog = require("plugins/dialog");

class searchSingleInputClause extends searchDialogViewModel {

    public applyFilterTask = $.Deferred();
    label = "";

    constructor(label: string) {
        super([ko.observable("")]);

        this.label = label;
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.inputs[0]());

        this.close();
    }
}

export = searchSingleInputClause;