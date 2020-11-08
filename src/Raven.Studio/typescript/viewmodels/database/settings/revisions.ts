import viewModelBase = require("viewmodels/viewModelBase");
import revisionsConfigurationEntry = require("models/database/documents/revisionsConfigurationEntry");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import saveRevisionsConfigurationCommand = require("commands/database/documents/saveRevisionsConfigurationCommand");
import saveRevisionsForConflictsConfigurationCommand = require("commands/database/documents/saveRevisionsForConflictsConfigurationCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import messagePublisher = require("common/messagePublisher");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getRevisionsConfigurationCommand = require("commands/database/documents/getRevisionsConfigurationCommand");
import getRevisionsForConflictsConfigurationCommand = require("commands/database/documents/getRevisionsForConflictsConfigurationCommand");
import enforceRevisionsConfigurationCommand = require("commands/database/settings/enforceRevisionsConfigurationCommand");
import notificationCenter = require("common/notifications/notificationCenter");
import popoverUtils = require("common/popoverUtils");

class revisions extends viewModelBase {

    defaultDocumentConfiguration = ko.observable<revisionsConfigurationEntry>();
    defaultConflictConfiguration = ko.observable<revisionsConfigurationEntry>();
    
    perCollectionConfigurations = ko.observableArray<revisionsConfigurationEntry>([]);

    isSaveEnabled: KnockoutComputed<boolean>;
    collections = collectionsTracker.default.collections;
    selectionState: KnockoutComputed<checkbox>;
    selectedItems = ko.observableArray<revisionsConfigurationEntry>([]);

    currentlyEditedItem = ko.observable<revisionsConfigurationEntry>(); // reference to cloned and currently being edited item
    currentBackingItem = ko.observable<revisionsConfigurationEntry>(null); // original item which is edited

    revertRevisionsUrl: KnockoutComputed<string>;
    enforceButtonTitle: KnockoutComputed<string>;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        this.bindToCurrentInstance("createDefaultConfiguration", "saveChanges",
            "deleteItem", "editItem", "applyChanges",
            "exitEditMode", "enableConfiguration",
            "disableConfiguration", "toggleSelectAll", "enforceConfiguration");

        this.initObservables();
    }

    private initObservables() {
        this.selectionState = ko.pureComputed<checkbox>(() => {
            const selectedCount = this.selectedItems().length;
            const totalCount = this.perCollectionConfigurations().length + (this.defaultDocumentConfiguration() ? 1 : 0);
            if (totalCount && selectedCount === totalCount)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });
        
        this.revertRevisionsUrl = ko.pureComputed(() => {
            return appUrl.forRevertRevisions(this.activeDatabase());
        });
        
        this.enforceButtonTitle = ko.pureComputed(() => {
            if (this.isSaveEnabled()) {
                return "Save current configuration before enforcing";
            }
            return "Enforce the defined revisions configuration on all documents per collection";
        });
    }

    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const db = this.activeDatabase();
                
                const revisionsTask = this.fetchRevisionsConfiguration(db);
                const conflictsTask = this.fetchRevisionsForConflictsConfiguration(db);
                
                return $.when<any>(revisionsTask, conflictsTask)
                    .then(() => ({ can: true }))
                    .fail(() => ({ redirect: appUrl.forDatabaseRecord(this.activeDatabase()) }));
            });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("1UZ5WL");

        this.dirtyFlag = new ko.DirtyFlag([this.perCollectionConfigurations, this.defaultDocumentConfiguration, this.defaultConflictConfiguration]);
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });
    }
    
    private fetchRevisionsForConflictsConfiguration(db: database): JQueryPromise<Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration> {
        return new getRevisionsForConflictsConfigurationCommand(db)
            .execute()
            .done((config: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration) => {
                this.onRevisionsForConflictsConfigurationLoaded(config);
            });
    }

    private fetchRevisionsConfiguration(db: database): JQueryPromise<Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration> {
        return new getRevisionsConfigurationCommand(db)
            .execute()
            .done((revisionsConfig: Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration) => {
                this.onRevisionsConfigurationLoaded(revisionsConfig);
            });
    }

    onRevisionsConfigurationLoaded(data: Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration) {
        if (data) {
            if (data.Default) {
                this.defaultDocumentConfiguration(new revisionsConfigurationEntry(revisionsConfigurationEntry.DefaultConfiguration, data.Default));
            }

            if (data.Collections) {
                this.perCollectionConfigurations(_.map(data.Collections, (configuration, collection) => {
                    return new revisionsConfigurationEntry(collection, configuration);
                }));
            }
            this.dirtyFlag().reset();
        } else {
            this.defaultDocumentConfiguration(null);
            this.perCollectionConfigurations([]);
        }
    }

    onRevisionsForConflictsConfigurationLoaded(data: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration) {
        if (data) {
            this.defaultConflictConfiguration(new revisionsConfigurationEntry(revisionsConfigurationEntry.ConflictsConfiguration, data));
            
            this.dirtyFlag().reset();
        } else {
            const dto: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration = {
                Disabled: false,
                PurgeOnDelete: false,
                MinimumRevisionsToKeep: undefined,
                MinimumRevisionAgeToKeep: "45.00:00:00"
            };
            this.defaultConflictConfiguration(new revisionsConfigurationEntry(revisionsConfigurationEntry.ConflictsConfiguration, dto));
        }
    }

    createCollectionNameAutocompleter(item: revisionsConfigurationEntry) {
        return ko.pureComputed(() => {
            const key = item.collection(); 
            const options = collectionsTracker.default.getCollectionNames();
            
            const usedOptions = this.perCollectionConfigurations().filter(f => f !== item).map(x => x.collection());

            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    createDefaultConfiguration() {
        eventsCollector.default.reportEvent("revisions", "create");

        this.editItem(revisionsConfigurationEntry.defaultConfiguration());
    }

    addCollectionSpecificConfiguration() {
        eventsCollector.default.reportEvent("revisions", "create");

        this.currentBackingItem(null);
        this.currentlyEditedItem(revisionsConfigurationEntry.empty());

        this.currentlyEditedItem().validationGroup.errors.showAllMessages(false);
    }

    removeConfiguration(entry: revisionsConfigurationEntry) {
        eventsCollector.default.reportEvent("revisions", "remove");

        if (entry.isDefault()) {
            this.defaultDocumentConfiguration(null);
        } else {
            this.perCollectionConfigurations.remove(entry);
        }
    }

    applyChanges() {
        const itemToSave = this.currentlyEditedItem();
        const isEdit = !!this.currentBackingItem();
        if (!this.isValid(itemToSave.validationGroup)) {
            return;
        }

        if (itemToSave.isDefault()) {
            this.defaultDocumentConfiguration(itemToSave);
            this.initTooltips();
        } else if (isEdit) {
            this.currentBackingItem().copyFrom(itemToSave);
        } else {
            this.perCollectionConfigurations.push(itemToSave);
        }

        this.exitEditMode();
    }

    toRevisionsDto(): Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration {
        const perCollectionConfigurations = this.perCollectionConfigurations();

        const collectionsDto = {} as { [key: string]: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration; }

        perCollectionConfigurations.forEach(config => {
            collectionsDto[config.collection()] = config.toDto();
        });

        return {
            Default: this.defaultDocumentConfiguration() ? this.defaultDocumentConfiguration().toDto() : null,
            Collections: collectionsDto
        }
    }

    saveChanges() {
        // first apply current changes:
        const itemBeingEdited = this.currentlyEditedItem();
        if (itemBeingEdited) {
            if (this.isValid(itemBeingEdited.validationGroup)) {
                this.applyChanges();
            } else {

                // we have validation error - stop saving
                return;
            }
        }

        this.spinners.save(true);

        eventsCollector.default.reportEvent("revisions", "save");

        const db = this.activeDatabase();
        
        const revisionsForConflictsDto = this.defaultConflictConfiguration().toDto();
        const conflictsTask = new saveRevisionsForConflictsConfigurationCommand(db, revisionsForConflictsDto)
            .execute();

        const revisionsDto = this.toRevisionsDto();
        const revisionsTask = new saveRevisionsConfigurationCommand(db, revisionsDto)
            .execute();
        
        const saveTasks = [conflictsTask, revisionsTask];
        
        $.when<any>(...saveTasks)
            .done(() => {
                this.dirtyFlag().reset();
                messagePublisher.reportSuccess(`Revisions configuration has been saved`);
            })
            .always(() => {
                this.spinners.save(false);
                const db = this.activeDatabase();
                db.hasRevisionsConfiguration(true);
                collectionsTracker.default.configureRevisions(db);
            });
    }

    compositionComplete() {
        super.compositionComplete();

        popoverUtils.longWithHover($(".conflict-defaults-info"),
            {
                content: `<ul class="margin-top margin-top-xs">
                              <li><small>This is the default revision configuration for <strong>conflicting documents only</strong>.</small></li>
                              <li><small>When enabled, a revision is created for each conflicting item.</small></li>
                              <li><small>A revision is also created for the conflict resolution document.</small></li>
                              <li><small>When a collection specific configuration is defined, it <strong>overrides</strong> these defaults.</li>
                          </ul>`,
                html: true
            });

        this.initTooltips();
        this.setupDisableReasons();
    }

    editItem(entry: revisionsConfigurationEntry) {
        this.currentBackingItem(entry);
        const clone = revisionsConfigurationEntry.empty().copyFrom(entry);
        this.currentlyEditedItem(clone);
    }

    deleteItem(entry: revisionsConfigurationEntry) {
        this.selectedItems.remove(entry);

        if (entry.isDefault()) {
            this.defaultDocumentConfiguration(null);
        } else {
            this.perCollectionConfigurations.remove(entry);
        }

        this.exitEditMode();
    }

    exitEditMode() {
        this.currentBackingItem(null);
        this.currentlyEditedItem(null);
    }

    enableConfiguration(entry: revisionsConfigurationEntry) {
        entry.disabled(false);
    }

    disableConfiguration(entry: revisionsConfigurationEntry) {
        entry.disabled(true);
    }

    enableSelected() {
        this.selectedItems().forEach(item => item.disabled(false));
    }

    disableSelected() {
        this.selectedItems().forEach(item => item.disabled(true));
    }

    toggleSelectAll() {
        eventsCollector.default.reportEvent("revisions", "toggle-select-all");
        const selectedCount = this.selectedItems().length;

        if (selectedCount > 0) {
            this.selectedItems([]);
        } else {
            const selectedItems = this.perCollectionConfigurations().slice();
            if (this.defaultDocumentConfiguration()) {
                selectedItems.push(this.defaultDocumentConfiguration());
            }

            this.selectedItems(selectedItems);
        }
    }

    formattedDurationObservable(observable: KnockoutObservable<number>) {
        return ko.pureComputed(() => generalUtils.formatTimeSpan(observable() * 1000));
    }

    enforceConfiguration() {
        const db = this.activeDatabase();

        this.confirmationMessage("Enforce Revisions Configuration",
            "<small>Clicking '<strong>Enforce</strong>' will enforce the current revisions configuration on <strong>all</strong> existing revisions in the database per collection.<br><br>" +
            "<strong>Note</strong>: Revisions might be removed depending on the current configuration rules.</small>",
            { buttons: ["Cancel", "Enforce Revisions Configuration"], html: true })
            .done (result => {
                if (result.can) {
                    new enforceRevisionsConfigurationCommand(db)
                        .execute()
                        .done((operationIdDto: operationIdDto) => {
                            const operationId = operationIdDto.OperationId;
                            notificationCenter.instance.openDetailsForOperationById(db, operationId);
                        });
                }
            });
    }
    
    private initTooltips() {
        popoverUtils.longWithHover($(".document-defaults-info"),
            {
                content: `<ul class="margin-top margin-top-xs">
                              <li><small>This is the default revision configuration for all <strong>non-conflicting documents</strong>.</small></li>
                              <li><small>When enabled, a revision is created for all non-conflicting documents.</small></li>
                              <li><small>When a collection specific configuration is defined, it <strong>overrides</strong> these defaults.</li>
                          </ul>`,
                html: true
            });
    }
}

export = revisions;
