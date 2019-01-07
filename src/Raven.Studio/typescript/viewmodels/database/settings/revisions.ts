import viewModelBase = require("viewmodels/viewModelBase");
import revisionsConfigurationEntry = require("models/database/documents/revisionsConfigurationEntry");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import saveRevisionsConfigurationCommand = require("commands/database/documents/saveRevisionsConfigurationCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import messagePublisher = require("common/messagePublisher");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getRevisionsConfigurationCommand = require("commands/database/documents/getRevisionsConfigurationCommand");

class revisions extends viewModelBase {

    defaultConfiguration = ko.observable<revisionsConfigurationEntry>();
    perCollectionConfigurations = ko.observableArray<revisionsConfigurationEntry>([]);
    isSaveEnabled: KnockoutComputed<boolean>;
    collections = collectionsTracker.default.collections;
    selectionState: KnockoutComputed<checkbox>;
    selectedItems = ko.observableArray<revisionsConfigurationEntry>([]);

    currentlyEditedItem = ko.observable<revisionsConfigurationEntry>(); // reference to cloned and currently being edited item
    currentBackingItem = ko.observable<revisionsConfigurationEntry>(null); // original item which is edited

    revertRevisionsUrl: KnockoutComputed<string>;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        this.bindToCurrentInstance("createDefaultConfiguration", "saveChanges",
            "deleteItem", "editItem", "applyChanges",
            "exitEditMode", "enableConfiguration",
            "disableConfiguration", "toggleSelectAll");

        this.initObservables();
    }

    private initObservables() {
        this.selectionState = ko.pureComputed<checkbox>(() => {
            const selectedCount = this.selectedItems().length;
            const totalCount = this.perCollectionConfigurations().length + (this.defaultConfiguration() ? 1 : 0);
            if (totalCount && selectedCount === totalCount)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });
        
        this.revertRevisionsUrl = ko.pureComputed(() => {
            return appUrl.forRevertRevisions(this.activeDatabase());
        });
    }

    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.fetchRevisionsConfiguration(this.activeDatabase())
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseRecord(this.activeDatabase()) }));

                return deferred;
            });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("1UZ5WL");

        this.dirtyFlag = new ko.DirtyFlag([this.perCollectionConfigurations, this.defaultConfiguration]);
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
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
                this.defaultConfiguration(new revisionsConfigurationEntry(revisionsConfigurationEntry.DefaultConfiguration, data.Default));
            }

            if (data.Collections) {
                this.perCollectionConfigurations(_.map(data.Collections, (configuration, collection) => {
                    return new revisionsConfigurationEntry(collection, configuration);
                }));
            }
            this.dirtyFlag().reset();
        } else {
            this.defaultConfiguration(null);
            this.perCollectionConfigurations([]);
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
            this.defaultConfiguration(null);
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
            this.defaultConfiguration(itemToSave);
        } else if (isEdit) {
            this.currentBackingItem().copyFrom(itemToSave);
        } else {
            this.perCollectionConfigurations.push(itemToSave);
        }

        this.exitEditMode();
    }

    toDto(): Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration {
        const perCollectionConfigurations = this.perCollectionConfigurations();

        const collectionsDto = {} as { [key: string]: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration; }

        perCollectionConfigurations.forEach(config => {
            collectionsDto[config.collection()] = config.toDto();
        });

        return {
            Default: this.defaultConfiguration() ? this.defaultConfiguration().toDto() : null,
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

        const dto = this.toDto();

        new saveRevisionsConfigurationCommand(this.activeDatabase(), dto)
            .execute()
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
            this.defaultConfiguration(null);
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
            if (this.defaultConfiguration()) {
                selectedItems.push(this.defaultConfiguration());
            }

            this.selectedItems(selectedItems);
        }
    }

    formatedDurationObservable(observable: KnockoutObservable<number>) {
        return ko.pureComputed(() => generalUtils.formatTimeSpan(observable() * 1000));
    }
}

export = revisions;
