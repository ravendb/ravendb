import searchDialogViewModel = require("viewmodels/filesystem/files/searchDialogViewModel");

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

    enabled(): boolean {
        return this.checkRequired(true);
    }
}

export = searchSingleInputClause;