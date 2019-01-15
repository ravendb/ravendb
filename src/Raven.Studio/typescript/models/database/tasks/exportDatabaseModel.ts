/// <reference path="../../../../typings/tsd.d.ts"/>

import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");

class exportDatabaseModel {

    includeDatabaseRecord = ko.observable(true);
    includeDocuments = ko.observable(true);
    includeConflicts = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeCompareExchange = ko.observable(true);
    includeCounters = ko.observable(true);
    includeAttachments = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    revisionsAreConfigured: KnockoutComputed<boolean>;
    encryptOutput = ko.observable<boolean>(false);

    exportFileName = ko.observable<string>();
    
    encryptionKey = ko.observable<string>();
    savedKeyConfirmation = ko.observable<boolean>(false);

    includeExpiredDocuments = ko.observable(true);
    removeAnalyzers = ko.observable(false);

    includeAllCollections = ko.observable(true);
    includedCollections = ko.observableArray<string>([]);

    transformScript = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    encryptionValidationGroup: KnockoutValidationGroup;
    exportDefinitionHasIncludes: KnockoutComputed<boolean>;

    constructor() {
        this.initValidation();
        this.initEncryptionValidation();

        this.includeDocuments.subscribe(documents => {
            if (!documents) {
                this.includeCounters(false);
                this.includeAttachments(false);
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
        if (this.includeCounters()) {
            operateOnTypes.push("Counters");
        }
        if (this.includeAttachments()) {
            operateOnTypes.push("Attachments");
        }

        return {
            Collections: this.includeAllCollections() ? null : this.includedCollections(),
            FileName: this.exportFileName(),
            IncludeExpired: this.includeExpiredDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            EncryptionKey: this.encryptOutput() ? this.encryptionKey() : undefined,
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType,
            MaxStepsForTransformScript: 10 * 1000
        } as Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide;
    }

    private initValidation() {
        this.exportDefinitionHasIncludes = ko.pureComputed(() => {
            return this.includeDatabaseRecord() 
                || this.includeAttachments() 
                || this.includeConflicts() 
                || this.includeIndexes() 
                || this.includeIdentities() 
                || this.includeCompareExchange() 
                || this.includeCounters() 
                || (this.includeRevisionDocuments() && this.revisionsAreConfigured()) 
                || this.includeDocuments();
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
    
    private initEncryptionValidation() {
        setupEncryptionKey.setupConfirmationValidation(this.savedKeyConfirmation);
        setupEncryptionKey.setupKeyValidation(this.encryptionKey);

        this.encryptionValidationGroup = ko.validatedObservable({
            savedKeyConfirmation: this.savedKeyConfirmation,
            encryptionKey: this.encryptionKey
        })        
    }
}

export = exportDatabaseModel;
