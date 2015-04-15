import searchDialogViewModel = require("viewmodels/filesystem/files/searchDialogViewModel");

class searchFileSizeRangeClause extends searchDialogViewModel {

    static inputRegexp =  /^(\d+)\s*(|[kmgKMG]b)$/;

    public applyFilterTask = $.Deferred();

    constructor() {
        super([ko.observable(""), ko.observable("")]);
    }


    applyFilter() {
        var filter = "__size_numeric:[" + this.convertInputStringToRangeValue(this.inputs[0]()) +
            " TO " + this.convertInputStringToRangeValue(this.inputs[1]()) + "]";
        this.applyFilterTask.resolve(filter);

        this.close();
    }

    private validateInput(input: string): boolean {
        return !input.trim() || searchFileSizeRangeClause.inputRegexp.test(input);
    }

    private convertInputStringToRangeValue(input: string) : string {

        if (!input)
            return "*";

        var match = searchFileSizeRangeClause.inputRegexp.exec(input);
        var value = parseInt(match[1]);
        var loweredCaseMultiplier = match[2].toLowerCase();
        var multiplier = this.getMultiplier(loweredCaseMultiplier);

        value *= multiplier;

        return "Lx" + value.toString();
    }

    private getMultiplier(value: string) {

        if (!value)
            return 1;

        if (value.indexOf("kb") > -1)
            return 1024;

        if (value.indexOf("mb") > -1)
            return 1024 * 1024;

        if (value.indexOf("gb") > -1)
            return 1024 * 1024 * 1024;

        return 1;
    }

    enabled(): boolean {
        return this.checkRequired(false)
            && this.validateInput(this.inputs[0]())
            && this.validateInput(this.inputs[1]());
    }
}

export = searchFileSizeRangeClause;