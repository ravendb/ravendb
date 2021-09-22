/// <reference path="../../../../typings/tsd.d.ts"/>
import moment = require("moment");
import genUtils = require("common/generalUtils");

class patchDocument {

    name = ko.observable<string>("");
    query = ko.observable<string>("");
    recentPatch = ko.observable<boolean>(false);

    queryHasError = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;

    constructor(dto: patchDto) {
        
        this.name(dto.Name);
        this.query(dto.Query);
        this.recentPatch(dto.RecentPatch);
        
        this.initValidation();
    }
    
    private initValidation() {
        this.query.extend({
            required: true,
            aceValidation: true,
            validation: [
                {
                    validator: () => !this.queryHasError(),
                    message: "The query part of your patch script failed to execute. Please check the query syntax."
                }]
        });
        
        this.validationGroup = ko.validatedObservable({
            query: this.query
        })
    }
    
    toDto(): storedPatchDto {
        const name = this.name();
        const query = this.query();

        return {
            Name: name,
            Query: query,
            RecentPatch: this.recentPatch(),
            ModificationDate: moment().format("YYYY-MM-DD HH:mm"),
            Hash: genUtils.hashCode(
                (name || "") +
                query
            )
        };
    }
    
    copyFrom(dto: patchDto) {
        this.name(dto.Name);
        this.query(dto.Query);
        this.recentPatch(dto.RecentPatch);
    }

    static empty() {
        return new patchDocument({
            Name: "",
            Query: "",
            RecentPatch: false,
            ModificationDate: null
        });
    }
}

export = patchDocument;
