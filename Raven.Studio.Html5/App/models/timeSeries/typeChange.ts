import timeSeriesType = require("models/timeSeries/timeSeriesType");

class typeChange {
    type = ko.observable<string>("");
    typeCustomValidityError: KnockoutComputed<string>;
    fields = ko.observableArray<string>();
    isNew = ko.observable<boolean>(false);

    constructor(type: timeSeriesType, isNew: boolean = false) {
        this.type(type.name);
        this.fields(type.fields);
        this.isNew(isNew);

        this.typeCustomValidityError = ko.computed(() => {
            var name = this.type();
            if (!$.trim(name))
                return "Type name cannot be empty";
            if (name.length > 255) {
                return "Type name length can't exceed 255 characters";
            }
            if (name.contains('\\')) {
                return "Type name cannot contain '\\' char";
            }
            return "";
        });
    }

    addField() {
        this.fields.push("");
    }

    removeField(field: string) {
        this.fields.remove(field);
    }
}

export = typeChange;