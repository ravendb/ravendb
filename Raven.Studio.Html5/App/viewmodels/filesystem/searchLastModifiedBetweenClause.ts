import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class searchLastModifiedBetweenClause extends dialogViewModelBase {
    //hveiras: for date picker ko bindings see: https://github.com/hugozap/knockoutjs-date-bindings
    public applyFilterTask = $.Deferred();
    minSizeText = ko.observable("");
    maxSizeText = ko.observable("");

    cancel() {
        dialog.close(this);
    }

    applyFilter() {
        this.applyFilterTask.resolve("");
        dialog.close(this);
    }
}

export = searchLastModifiedBetweenClause;