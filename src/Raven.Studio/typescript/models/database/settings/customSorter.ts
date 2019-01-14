/// <reference path="../../../../typings/tsd.d.ts"/>

import jsonUtil = require("common/jsonUtil");

class customSorter {
    name = ko.observable<string>();
    code = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;

    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        this.name(dto.Name);
        this.code(dto.Code);
        
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.name, 
            this.code
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private initValidation() {
        this.name.extend({
            required: true
        });
        
        this.code.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            code: this.code
        });
    }
    
    toDto(): Raven.Client.Documents.Queries.Sorting.SorterDefinition {
        return {
            Name: this.name(),
            Code: this.code()
        }
    }
    
    static empty() {
        return new customSorter({
            Name: "",
            Code: ""
        });
    }
}

export = customSorter;
