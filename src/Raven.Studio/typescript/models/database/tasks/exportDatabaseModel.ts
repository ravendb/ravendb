/// <reference path="../../../../typings/tsd.d.ts"/>

class exportDatabaseModel {

    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    
    exportFileName = ko.observable<string>();

    includeExpiredDocuments = ko.observable(false);
    removeAnalyzers = ko.observable(false);

    includeAllCollections = ko.observable(true);
    includedCollections = ko.observableArray<string>([]);

    transformScript = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;

    constructor() {
        this.initValidation();
    }

    toDto(): Raven.Client.Documents.Smuggler.DatabaseSmugglerOptions {
        const operateOnTypes: Array<Raven.Client.Documents.Smuggler.DatabaseItemType> = [];
        if (this.includeDocuments()) {
            operateOnTypes.push("Documents");
        }
        if (this.includeIndexes()) {
            operateOnTypes.push("Indexes");
        }
        if (this.includeIdentities()) {
            operateOnTypes.push("Identities");
        }
        if (this.includeRevisionDocuments()) {
            operateOnTypes.push("RevisionDocuments");
        }

        return {
            CollectionsToExport: this.includeAllCollections() ? null : this.includedCollections(),
            FileName: this.exportFileName(),
            IncludeExpired: this.includeExpiredDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType
        } as Raven.Client.Documents.Smuggler.DatabaseSmugglerOptions;
    }

    private initValidation() {
        this.transformScript.extend({
            aceValidation: true
        });
        
        this.validationGroup = ko.validatedObservable({
            transformScript: this.transformScript
        });
    }
}

export = exportDatabaseModel;
