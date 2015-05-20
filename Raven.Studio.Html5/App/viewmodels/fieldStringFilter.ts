import searchDialogViewModel = require("viewmodels/filesystem/files/searchDialogViewModel");
import dialog = require("plugins/dialog");

class fieldStringFilter extends searchDialogViewModel {
    filterOptions = ko.observableArray(["Starts with", "Ends with", "Contains", "Exact"]);
    selectedOption = ko.observable("Starts with");
    public applyFilterTask = $.Deferred();
    label = "";

    constructor(label: string) {
        super([ko.observable("")]);

        this.label = label;
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.inputs[0](), this.selectedOption());

        this.close();
    }

    enabled(): boolean {
        return this.checkRequired(true);
    }
}

export = fieldStringFilter; 