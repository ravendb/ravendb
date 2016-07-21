import searchDialogViewModel = require("viewmodels/filesystem/files/searchDialogViewModel");

type unitItem = {
    name: string;
    multiplier: number;
}

class searchFileSizeRangeClause extends searchDialogViewModel {

    public applyFilterTask = $.Deferred();

    units: unitItem[] = [
        { name: 'b', multiplier: 1 },
        { name: 'Kb', multiplier: 1024 },
        { name: 'Mb', multiplier: 1024 * 1024 },
        { name: 'Gb', multiplier: 1024 * 1024 * 1024 }
    ];

    sizeFromUnit = ko.observable<unitItem>(this.units[0]);
    sizeToUnit = ko.observable<unitItem>(this.units[0]);

    constructor() {
        super([ko.observable(""), ko.observable("")]);
    }

    applyFilter() {
        var filter = "__size_numeric:[" + this.convertInputStringToRangeValue(this.inputs[0](), this.sizeFromUnit()) +
            " TO " + this.convertInputStringToRangeValue(this.inputs[1](), this.sizeToUnit()) + "]";
        this.applyFilterTask.resolve(filter);

        this.close();
    }

    private convertInputStringToRangeValue(input: string, unit: unitItem) : string {
        if (!input)
            return "*";

        var value = parseInt(input) * unit.multiplier;

        return "Lx" + value.toString();
    }

    enabled(): boolean {
        return this.checkRequired(false);
    }
}

export = searchFileSizeRangeClause;
