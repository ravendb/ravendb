import jsonUtil from "common/jsonUtil";

class compoundField {
    field1 = ko.observable<string>();
    field2 = ko.observable<string>();

    dirtyFlag: () => DirtyFlag;
    validationGroup: KnockoutObservable<any>;
    
    constructor() {
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.field1,
            this.field2,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private initValidation() {
        this.field1.extend({
            required: true
        });

        this.field2.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            field1: this.field1,
            field2: this.field2
        });
    }
    
    static fromDto(fields: string[]) {
        const field = new compoundField();
        field.field1(fields[0]);
        field.field2(fields[1]);
        field.dirtyFlag().reset();
        return field;
    }
}


export = compoundField;
