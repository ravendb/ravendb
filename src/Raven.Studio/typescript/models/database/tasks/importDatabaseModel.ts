/// <reference path="../../../../typings/tsd.d.ts"/>

class importDatabaseModel {
    includeDatabaseRecord = ko.observable(true);
    includeDocuments = ko.observable(true);
    includeConflicts = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeCompareExchange = ko.observable(true);
    includeCounters = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    includeLegacyAttachments = ko.observable(false);
    includeAttachments = ko.observable(true);

    includeExpiredDocuments = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    
    transformScript = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    importDefinitionHasIncludes: KnockoutComputed<boolean>;
    
    constructor() {
        this.initValidation();

        this.includeDocuments.subscribe(documents => {
            if (!documents) {
                this.includeCounters(false);
                this.includeAttachments(false);
                this.includeLegacyAttachments(false);
            }
        });
        
        this.removeAnalyzers.subscribe(analyzers => {
            if (analyzers) {
                this.includeIndexes(true);
            }
        });
        
        this.includeIndexes.subscribe(indexes => {
            if (!indexes) {
                this.removeAnalyzers(false);
            }
        });
    }
    
    toDto(): Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions {
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
        if (this.includeIdentities()){
            operateOnTypes.push("Identities");
        }
        if (this.includeCompareExchange()) {
            operateOnTypes.push("CompareExchange");
        }
        if (this.includeCounters()) {
            operateOnTypes.push("Counters");
        }
        if (this.includeAttachments()) {
            operateOnTypes.push("Attachments");
        }
        if (this.includeLegacyAttachments()) {
            operateOnTypes.push("LegacyAttachments");
        }

        return {
            IncludeExpired: this.includeExpiredDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType
        } as Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions;
    }

    private initValidation() {
        this.importDefinitionHasIncludes = ko.pureComputed(() => {
            return this.includeDatabaseRecord() 
                || this.includeConflicts() 
                || this.includeIndexes() 
                || this.includeIdentities() 
                || this.includeCompareExchange() 
                || this.includeLegacyAttachments() 
                || this.includeCounters() 
                || this.includeRevisionDocuments() 
                || this.includeDocuments()
                || this.includeAttachments();
        });

        this.transformScript.extend({
            aceValidation: true
        });

        this.importDefinitionHasIncludes.extend({
            validation: [
                {
                    validator: () => this.importDefinitionHasIncludes(),
                    message: "Note: At least one 'include' option must be checked..."
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            transformScript: this.transformScript,
            importDefinitionHasIncludes: this.importDefinitionHasIncludes
        });
    }
}

export = importDatabaseModel;
