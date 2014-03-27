import customColumnParams = require("models/customColumnParams");

class customColumns {

    columns = ko.observableArray<customColumnParams>();

    constructor(dto: customColumnsDto) {
        this.columns($.map(dto.Columns, c => new customColumnParams(c)));
    }

    static empty() {
        return new customColumns({ Columns: [] });
    }

    hasOverrides() {
        return this.columns().length > 0;
    }

    findConfigFor(binding: string): customColumnParams {
        var colParams = this.columns();
        for (var i = 0; i < colParams.length; i++) {
            var colParam = colParams[i];
            if (colParam.binding() === binding) {
                return colParam;
            }
        }
        return null;
    }

}

export = customColumns;