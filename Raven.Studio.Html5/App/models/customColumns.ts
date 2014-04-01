import customColumnParams = require("models/customColumnParams");
import document = require("models/document");

class customColumns {

    columns = ko.observableArray<customColumnParams>();
    customMode = ko.observable(false);
    enabled = ko.observable(true);

    constructor(dto: customColumnsDto) {
        this.columns($.map(dto.Columns, c => new customColumnParams(c)));
        this.customMode(true);
    }

    static empty() {
        return new customColumns({ Columns: [] });
    }

    copyFrom(src: customColumns) {
        this.columns(src.columns());
        this.customMode(src.customMode());
    }

    clone(): customColumns {
        var copy = new customColumns(this.toDto());
        copy.customMode(this.customMode());
        return copy;
    }

    hasOverrides() {
        return this.enabled() && this.customMode() && this.columns().length > 0;
    }

    toDto(): customColumnsDto {
        return {
            'Columns': $.map(this.columns(), c => c.toDto())
        };
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