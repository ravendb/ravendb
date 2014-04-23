import filesystem = require("models/filesystem/filesystem");
import datePickerBindingHandler = require("common/datePickerBindingHandler");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import moment = require("moment");

class searchLastModifiedBetweenClause extends dialogViewModelBase {

    public applyFilterTask = $.Deferred();
    fromDate = ko.observable<Moment>();
    toDate = ko.observable<Moment>();
    fromDateText: KnockoutComputed<string>;
    toDateText: KnockoutComputed<string>;

    constructor(private fs: filesystem) {
        super();

        this.fromDateText = ko.computed(function () {
            return this.fromDate() != null ? this.fromDate().format("YYYY/MM/DD") : "";
        }, this);

        this.toDateText = ko.computed(function () {
            return this.toDate() != null ? this.toDate().format("YYYY/MM/DD") : "";
        }, this);

        datePickerBindingHandler.install();
    }

    cancel() {
        dialog.close(this);
    }

    applyFilter() {
        if (this.fromDate() == null || this.toDate() == null)
            return false;
        var dates = "__modified:[" + this.fromDate().format("YYYY/MM/DD").replaceAll("/", "-")
            + "_00-00-00" + " TO " + this.toDate().format("YYYY/MM/DD").replaceAll("/", "-") + "_23-59-59" + "]";
        this.applyFilterTask.resolve(dates);
        dialog.close(this);
    }
}

export = searchLastModifiedBetweenClause;