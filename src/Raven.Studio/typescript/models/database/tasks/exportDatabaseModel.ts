/// <reference path="../../../../typings/tsd.d.ts"/>

class exportDatabaseModel {

    includeDatabaseRecord = ko.observable(true);
    includeDocuments = ko.observable(true);
    includeConflicts = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeCompareExchange = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    revisionsAreConfigured: KnockoutComputed<boolean>;

    exportFileName = ko.observable<string>();

    includeExpiredDocuments = ko.observable(true);
    removeAnalyzers = ko.observable(false);

    includeAllCollections = ko.observable(true);
    includedCollections = ko.observableArray<string>([]);

    transformScript = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    exportDefinitionHasIncludes: KnockoutComputed<boolean>;

    constructor() {
        this.initValidation();
    }

    toDto(): Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide {
        const operateOnTypes: Array<Raven.Client.Documents.Smuggler.DatabaseItemType> = [];
        if (this.includeDatabaseRecord()) {
            operateOnTypes.push("DatabaseRecord");
        }
        if (this.includeDocuments()) {
            operateOnTypes.push("Documents");
        }
        if (this.includeConflicts()) {
            operateOnTypes.push("Conflicts");
        }
        if (this.includeIndexes()) {
            operateOnTypes.push("Indexes");
        }
        if (this.includeRevisionDocuments()) {
            operateOnTypes.push("RevisionDocuments");
        }
        if (this.includeIdentities()) {
            operateOnTypes.push("Identities");
        }
        if (this.includeCompareExchange()) {
            operateOnTypes.push("CompareExchange");
        }

        return {
            Collections: this.includeAllCollections() ? null : this.includedCollections(),
            FileName: this.exportFileName(),
            IncludeExpired: this.includeExpiredDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType,
            MaxStepsForTransformScript: 10 * 1000
        } as Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide;
    }

    private initValidation() {
        this.exportDefinitionHasIncludes = ko.pureComputed(() => {
            return this.includeDatabaseRecord() || this.includeDocuments() || (this.includeRevisionDocuments() && this.revisionsAreConfigured()) || this.includeConflicts() ||
                this.includeIndexes() || this.includeIdentities() || this.includeCompareExchange();
        });

        this.transformScript.extend({
            aceValidation: true
        });

        this.exportDefinitionHasIncludes.extend({
            validation: [
                {
                    validator: () => this.exportDefinitionHasIncludes(),
                    message: "Note: At least one 'include' option must be checked..."
                }
            ]
        });
       
        this.validationGroup = ko.validatedObservable({
            transformScript: this.transformScript,
            exportDefinitionHasIncludes: this.exportDefinitionHasIncludes
        });
    }
}

export = exportDatabaseModel;
