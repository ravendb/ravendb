/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import jsonUtil = require("common/jsonUtil");

class revisionsConfigurationEntry {

    static readonly DefaultConfiguration = "DefaultConfiguration";
    static readonly ConflictsConfiguration = "ConflictsConfiguration";

    disabled = ko.observable<boolean>();
    purgeOnDelete = ko.observable<boolean>();
    collection = ko.observable<string>();

    limitRevisions = ko.observable<boolean>();
    minimumRevisionsToKeep = ko.observable<number>();
    minimumRevisionsToKeepCurrent = ko.observable<number>();

    limitRevisionsByAge = ko.observable<boolean>(false);
    minimumRevisionAgeToKeep = ko.observable<number>();
    minimumRevisionAgeToKeepCurrent = ko.observable<number>();

    setMaxRevisionsToDelete = ko.observable<boolean>(false);
    maxRevisionsToDeleteUponUpdate = ko.observable<number>();

    isDefault: KnockoutComputed<boolean>;
    isConflicts: KnockoutComputed<boolean>;

    canChangeName: KnockoutObservable<boolean>;

    deleteDescription: KnockoutComputed<string>;
    humaneRetentionDescription: KnockoutComputed<string>;

    name: KnockoutComputed<string>;

    showLimitRevisionsWarning: KnockoutComputed<boolean>;
    showLimitRevisionsByAgeWarning: KnockoutComputed<boolean>;
    
    private static readonly revisionsDelta = 100;
    private static readonly revisionsByAgeDelta = 604800; // 7 days
    
    limitWarningHtml = (byAge: boolean = false) => `The new limit is much lower than the current value (delta > 
                        ${byAge ? generalUtils.formatTimeSpan(revisionsConfigurationEntry.revisionsByAgeDelta * 1000, true) : revisionsConfigurationEntry.revisionsDelta}).<br>
                        It is advised to set the # of revisions to delete upon document update.`
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        collection: this.collection,
        minimumRevisionsToKeep: this.minimumRevisionsToKeep,
        minimumRevisionAgeToKeep: this.minimumRevisionAgeToKeep,
        maxRevisionsToDeleteUponUpdate: this.maxRevisionsToDeleteUponUpdate
    });

    dirtyFlag: () => DirtyFlag;

    constructor(collection: string, dto: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration) {
        this.collection(collection);

        this.limitRevisions(dto.MinimumRevisionsToKeep != null);
        this.minimumRevisionsToKeep(dto.MinimumRevisionsToKeep);
        this.minimumRevisionsToKeepCurrent(dto.MinimumRevisionsToKeep);

        this.limitRevisionsByAge(dto.MinimumRevisionAgeToKeep != null);
        this.minimumRevisionAgeToKeep(dto.MinimumRevisionAgeToKeep ? generalUtils.timeSpanToSeconds(dto.MinimumRevisionAgeToKeep) : null);
        this.minimumRevisionAgeToKeepCurrent(this.minimumRevisionAgeToKeep());

        this.setMaxRevisionsToDelete(dto.MaximumRevisionsToDeleteUponDocumentUpdate != null);
        this.maxRevisionsToDeleteUponUpdate(dto.MaximumRevisionsToDeleteUponDocumentUpdate);

        this.disabled(dto.Disabled);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.isDefault = ko.pureComputed<boolean>(() => this.collection() === revisionsConfigurationEntry.DefaultConfiguration);
        this.isConflicts = ko.pureComputed<boolean>(() => this.collection() === revisionsConfigurationEntry.ConflictsConfiguration);
        
        this.canChangeName = ko.pureComputed<boolean>(() => {
            const isDefault = this.isDefault();
            const isConflicts = this.isConflicts();
            
            return !isDefault && !isConflicts;
        });

        this.initObservables();
        this.initValidation();
    }
    
    private initObservables() {
        this.showLimitRevisionsWarning = ko.pureComputed(() => {
            return this.minimumRevisionsToKeepCurrent() &&
                this.limitRevisions() &&
                this.minimumRevisionsToKeep() &&
                !this.setMaxRevisionsToDelete() &&
                (this.minimumRevisionsToKeepCurrent() - this.minimumRevisionsToKeep() > revisionsConfigurationEntry.revisionsDelta);
        });
        
        this.showLimitRevisionsByAgeWarning = ko.pureComputed(() => {
            return this.minimumRevisionAgeToKeepCurrent() &&
                this.limitRevisionsByAge() &&
                this.minimumRevisionAgeToKeep() &&
                !this.setMaxRevisionsToDelete() &&
                (this.minimumRevisionAgeToKeepCurrent() - this.minimumRevisionAgeToKeep() > revisionsConfigurationEntry.revisionsByAgeDelta);
        });
        
        this.deleteDescription = ko.pureComputed(() => {
            const purgeOffText = `<li>A revision will be created anytime a document is modified or deleted.</li>
                                  <li>Revisions of a deleted document can be accessed in the Revisions Bin view.</li>`;

            const purgeOnText = `<li>A revision will be created anytime a document is modified.</li>
                                 <li>When a document is deleted all its revisions will be removed.</li>`;

            let description = this.purgeOnDelete() ? purgeOnText : purgeOffText;
            return `<ul class="margin-top-sm">${description}</ul>`; 
        });

        this.humaneRetentionDescription = ko.pureComputed(() => {
            const retentionTimeHumane = generalUtils.formatTimeSpan(this.minimumRevisionAgeToKeep() * 1000, true);
            
            const limitByNumber = this.limitRevisions() && this.minimumRevisionsToKeep.isValid();
            const limitByAge = this.limitRevisionsByAge() && this.minimumRevisionAgeToKeep.isValid();
            const maxRevisionsToDelete = this.setMaxRevisionsToDelete() && this.maxRevisionsToDeleteUponUpdate.isValid();
            
            let description = "";
            
            if (limitByNumber && !limitByAge) {
                description = `<li>Only the latest <strong>${this.minimumRevisionsToKeep()}</strong> revisions will be kept.</li>
                               <li>Older revisions will be removed on next revision creation.</li>`;
            }
            
            if (!limitByNumber && limitByAge) {
                description = `<li>Revisions that exceed <strong>${retentionTimeHumane}</strong> will be removed on next revision creation.</li>`;
            }

            if (limitByNumber && limitByAge) {
                description =  `<li>At least <strong>${this.minimumRevisionsToKeep()}</strong> of the latest revisions will be kept.</li>
                                <li>Older revisions will be removed if they exceed <strong>${retentionTimeHumane}</strong> on next revision creation.</li>`;
            }
                
            if (maxRevisionsToDelete && (limitByNumber || limitByAge)) {
                description += `<li>A maximum of <strong>${this.maxRevisionsToDeleteUponUpdate()}</strong> revisions will be deleted each time a document is updated,
                                    until the defined '# of revisions to keep' limit is reached.
                                </li>`
            }
                
            return description ? `<ul class="margin-top-sm">${description}</ul>` : "";
        });

        this.limitRevisions.subscribe(() => {
            this.minimumRevisionsToKeep.clearError();
        });

        this.limitRevisionsByAge.subscribe(() => {
            this.minimumRevisionAgeToKeep.clearError();
        });

        this.setMaxRevisionsToDelete.subscribe(() => {
            this.maxRevisionsToDeleteUponUpdate.clearError();
        });
        
        this.name = ko.pureComputed(() => {
            if (this.isDefault()) {
                return "Document Defaults";
            } 
            if (this.isConflicts()) {
                return "Conflicting Document Defaults";
            }
            
            return this.collection();
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.collection,
            this.disabled,
            this.purgeOnDelete,
            
            this.limitRevisions,
            this.minimumRevisionsToKeep,
            
            this.limitRevisionsByAge,
            this.minimumRevisionAgeToKeep,
            
            this.setMaxRevisionsToDelete,
            this.maxRevisionsToDeleteUponUpdate
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidation() {
        this.collection.extend({
            required: true
        });

        this.minimumRevisionsToKeep.extend({
            required: {
                onlyIf: () => this.limitRevisions()
            },
            digit: true
        });

        this.minimumRevisionAgeToKeep.extend({
            required: {
                onlyIf: () => this.limitRevisionsByAge()
            },
            min: 0
        });

        this.maxRevisionsToDeleteUponUpdate.extend({
            required: {
                onlyIf: () => this.setMaxRevisionsToDelete()
            },
            digit: true
        });
    }

    copyFrom(incoming: revisionsConfigurationEntry): this {
        this.disabled(incoming.disabled());
        this.purgeOnDelete(incoming.purgeOnDelete());
        this.collection(incoming.collection());

        this.limitRevisions(incoming.limitRevisions());
        this.minimumRevisionsToKeep(incoming.minimumRevisionsToKeep());
        this.minimumRevisionsToKeepCurrent(incoming.minimumRevisionsToKeepCurrent());

        this.limitRevisionsByAge(incoming.limitRevisionsByAge());
        this.minimumRevisionAgeToKeep(incoming.minimumRevisionAgeToKeep());
        this.minimumRevisionAgeToKeepCurrent(incoming.minimumRevisionAgeToKeepCurrent());
        
        this.setMaxRevisionsToDelete(incoming.setMaxRevisionsToDelete());
        this.maxRevisionsToDeleteUponUpdate(incoming.maxRevisionsToDeleteUponUpdate());
        
        return this;
    }

    toDto(): Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration {
        const minimumRevisionsToKeep = this.limitRevisions() ? this.minimumRevisionsToKeep() : null;
        const minimumRevisionsAgeToKeep = this.limitRevisionsByAge() ? generalUtils.formatAsTimeSpan(this.minimumRevisionAgeToKeep() * 1000) : null;
        const maxRevisionsToDelete = this.setMaxRevisionsToDelete() ? this.maxRevisionsToDeleteUponUpdate() : null; 
        
        return {
            Disabled: this.disabled(),
            MinimumRevisionsToKeep: minimumRevisionsToKeep,
            MinimumRevisionAgeToKeep: minimumRevisionsAgeToKeep,
            MaximumRevisionsToDeleteUponDocumentUpdate: minimumRevisionsToKeep || minimumRevisionsAgeToKeep ? maxRevisionsToDelete : null,
            PurgeOnDelete: this.purgeOnDelete()
        };
    }

    static empty() {
        return new revisionsConfigurationEntry("",
        {
            Disabled: false,
            MinimumRevisionsToKeep: null,
            MinimumRevisionAgeToKeep: null,
            MaximumRevisionsToDeleteUponDocumentUpdate: null,
            PurgeOnDelete: false
        });
    }

    static defaultConfiguration() {
        const item = revisionsConfigurationEntry.empty();
        item.collection(revisionsConfigurationEntry.DefaultConfiguration);
        return item;
    }
}

export = revisionsConfigurationEntry;
