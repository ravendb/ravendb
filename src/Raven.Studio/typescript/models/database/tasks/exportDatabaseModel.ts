/// <reference path="../../../../typings/tsd.d.ts"/>
import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import smugglerDatabaseRecord = require("models/database/tasks/smugglerDatabaseRecord");
import genUtils = require("common/generalUtils");

class exportDatabaseModel {
    includeDatabaseRecord = ko.observable(true);
    includeDocuments = ko.observable(true);
    includeConflicts = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeCompareExchange = ko.observable(true);
    includeCounters = ko.observable(true);
    includeAttachments = ko.observable(true);
    includeTimeSeries = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    includeSubscriptions = ko.observable(true);
    
    encryptOutput = ko.observable<boolean>(false);
    
    databaseModel = new smugglerDatabaseRecord();
    
    exportFileName = ko.observable<string>();
    
    encryptionKey = ko.observable<string>();
    savedKeyConfirmation = ko.observable<boolean>(false);

    includeExpiredDocuments = ko.observable(true);
    includeArtificialDocuments = ko.observable(false);
    removeAnalyzers = ko.observable(false);

    includeAllCollections = ko.observable(true);
    includedCollections = ko.observableArray<string>([]);

    transformScript = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    encryptionValidationGroup: KnockoutValidationGroup;
    exportDefinitionHasIncludes: KnockoutComputed<boolean>;
    itemsToWarnAbout: KnockoutComputed<string>;

    constructor() {
        this.initValidation();
        this.initEncryptionValidation();
        this.initObservables();
    }

    private initObservables() {
        this.includeDocuments.subscribe(documents => {
           if (!documents) {
               this.includeAttachments(false);
           } 
        });
        
        this.includeCounters.subscribe(counters => {
            if (counters) {
                this.includeDocuments(true);
            }
        });
        
        this.includeAttachments.subscribe(attachments => {
            if (attachments) {
                this.includeDocuments(true);
            }
        });
        
        this.includeTimeSeries.subscribe(timeSeries => {
            if (timeSeries) {
                this.includeDocuments(true);
            }
        });

        this.includeRevisionDocuments.subscribe(revisions => {
            if (revisions) {
                this.includeDocuments(true);
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

        this.includeDatabaseRecord.subscribe(dbRecord => {
            if (!dbRecord) {
                this.databaseModel.customizeDatabaseRecordTypes(false);
            }
        });

        this.databaseModel.customizeDatabaseRecordTypes.subscribe(customize => {
            if (customize) {
                this.includeDatabaseRecord(true);
            }
        })

        this.itemsToWarnAbout = ko.pureComputed(() => {
            let items = [];

            if (!this.includeDocuments()) {
                if (this.includeCounters()) {
                    items.push("Counters");
                }
                if (this.includeTimeSeries()) {
                    items.push("Time Series");
                }
                if (this.includeRevisionDocuments()) {
                    items.push("Revisions");
                }
            }

            return genUtils.getItemsListFormatted(items);
        });
    }

    toDto(): Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide {
        const operateOnTypes: Array<Raven.Client.Documents.Smuggler.DatabaseItemType> = [];
        const databaseRecordTypes = this.databaseModel.getDatabaseRecordTypes();
        
        if (this.includeDatabaseRecord() && databaseRecordTypes.length) {
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
            operateOnTypes.push("CounterGroups");
        }
        if (this.includeAttachments()) {
            operateOnTypes.push("Attachments");
        }
        if (this.includeTimeSeries()) {
            operateOnTypes.push("TimeSeries");
        }
        if (this.includeSubscriptions()) {
            operateOnTypes.push("Subscriptions");
        }

        const recordTypes = (databaseRecordTypes.length ? databaseRecordTypes.join(",") : undefined) as Raven.Client.Documents.Smuggler.DatabaseRecordItemType;
        
        return {
            Collections: this.includeAllCollections() ? null : this.includedCollections(),
            FileName: this.exportFileName(),
            IncludeExpired: this.includeExpiredDocuments(),
            IncludeArtificial: this.includeArtificialDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            EncryptionKey: this.encryptOutput() ? this.encryptionKey() : undefined,
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType,
            OperateOnDatabaseRecordTypes: recordTypes,
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
                || this.includeSubscriptions()
                || this.includeCompareExchange() 
                || this.includeCounters() 
                || this.includeRevisionDocuments()
                || this.includeTimeSeries()
                || this.includeDocuments()
                || this.includeArtificialDocuments();
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
            exportDefinitionHasIncludes: this.exportDefinitionHasIncludes,
            databaseRecordHasIncludes: this.databaseModel.hasIncludes
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
