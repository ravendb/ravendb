import searchDialogViewModel = require("viewmodels/filesystem/files/searchDialogViewModel");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import moment = require("moment");
import dialog = require("plugins/dialog");

class fieldRangeFilter extends searchDialogViewModel {
    filterOptions = ko.observableArray(["Numeric Double", "Numeric Int","Numeric Long", "Alphabetical", "Datetime"]);
    selectedOption = ko.observable("Starts with");
    public applyFilterTask = $.Deferred();
    label = "";
    from = ko.observable();
    to = ko.observable();
    fromDate = ko.observable<moment.Moment>();
    toDate = ko.observable<moment.Moment>();
    constructor(label: string) {
        super([ko.observable("")]);
        datePickerBindingHandler.install();
        this.label = label;
        this.from("");
        this.to("");
        this.fromDate.subscribe(v =>
            this.from(this.fromDate() != null ? this.fromDate().format("YYYY-MM-DDTHH:mm:00.0000000") : ""));

        this.toDate.subscribe(v =>
            this.to(this.toDate() != null ? this.toDate().format("YYYY-MM-DDTHH:mm:00.0000000") : ""));
    }

    applyFilter() {
        this.applyFilterTask.resolve(this.from(), this.to(), this.selectedOption());

        this.close();
    }

    enabled(): boolean {
        return true;
    }

    isDateTime(): boolean {
        return (this.selectedOption() === "Datetime");
    }
}

export = fieldRangeFilter;  
