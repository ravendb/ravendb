/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class schemaDefinitionModel {

    collectionName = ko.observable<string>();
    schemaText = ko.observable<string>("");

    dirtyFlag: () => DirtyFlag;
    validationGroup: KnockoutObservable<any>;

    constructor(collectionName: string = "", schemaText: string = "") {
        this.collectionName(collectionName);
        this.schemaText(schemaText);

        this.initObservables();
        this.initValidation();
    }

    private initObservables() {
        this.dirtyFlag = new ko.DirtyFlag([
            this.collectionName,
            this.schemaText
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidation() {
        // TODO Maksym: add unique collection validation
        this.collectionName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            collectionName: this.collectionName
        });
    }

    static empty(): schemaDefinitionModel {
        return new schemaDefinitionModel();
    }

    toDto(): { collectionName: string; schemaText: string } {
        return {
            collectionName: this.collectionName(),
            schemaText: this.schemaText()
        };
    }
}

export = schemaDefinitionModel;

