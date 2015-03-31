import filesystem = require("models/filesystem/filesystem");
import datePickerBindingHandler = require("common/datePickerBindingHandler");
import searchDialogViewModel = require("viewmodels/filesystem/searchDialogViewModel");
import dialog = require("plugins/dialog");
import moment = require("moment");

class searchLastModifiedBetweenClause extends searchDialogViewModel {

    public applyFilterTask = $.Deferred();
    fromDate = ko.observable<Moment>();
    toDate = ko.observable<Moment>();

    fromDateText = ko.observable<string>();
    toDateText = ko.observable<string>();

    constructor() {
        super([]);

        this.inputs.push(this.fromDateText);
        this.inputs.push(this.toDateText);

        this.fromDate.subscribe(v =>
            this.fromDateText(this.fromDate() != null ? this.fromDate().format("YYYY-MM-DD HH-mm-ss") : ""));

        this.toDate.subscribe(v =>
            this.toDateText(this.toDate() != null ? this.toDate().format("YYYY-MM-DD HH-mm-ss") : ""));

        datePickerBindingHandler.install();
    }

    applyFilter() {
        if (!this.fromDateText() || !this.toDateText())
            return false;
        var dates = "__modified:[" + this.fromDateText().trim().replaceAll(" ", "_")
            + " TO " + this.toDateText().trim().replaceAll(" ", "_") + "]";
        this.applyFilterTask.resolve(dates);
        
        this.close()
    }

}

export = searchLastModifiedBetweenClause;