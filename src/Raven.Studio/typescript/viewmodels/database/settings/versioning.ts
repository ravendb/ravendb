import viewModelBase = require("viewmodels/viewModelBase");
import versioningEntry = require("models/database/documents/versioningEntry");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import saveVersioningCommand = require("commands/database/documents/saveVersioningCommand");
import eventsCollector = require("common/eventsCollector");
import messagePublisher = require("common/messagePublisher");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getVersioningCommand = require("commands/database/documents/getVersioningCommand");

class versioning extends viewModelBase {

    static readonly versioningDocumentKey = "Raven/Versioning/Configuration";

    defaultVersioning = ko.observable<versioningEntry>(versioningEntry.defaultConfiguration());
    versionings = ko.observableArray<versioningEntry>();
    isSaveEnabled: KnockoutComputed<boolean>;
    versioningEnabled = ko.observable<boolean>(false);
    collections = collectionsTracker.default.collections;

    constructor() {
        super();

        this.bindToCurrentInstance("removeVersioning", "createNewVersioning");
    }

    spinners = {
        save: ko.observable<boolean>(false)
    }

    canActivate(args: any) {
        super.canActivate(args);

        const deferred = $.Deferred<canActivateResultDto>();

        this.fetchVersioningEntries(this.activeDatabase())
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));

        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("1UZ5WL");

        this.dirtyFlag = new ko.DirtyFlag([this.versionings, this.defaultVersioning, this.versioningEnabled]);
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });
    }

    compositionComplete(): void {
        super.compositionComplete();

        this.setupDisableReasons();
    }

    private fetchVersioningEntries(db: database): JQueryPromise<Raven.Client.Server.Versioning.VersioningConfiguration> {
        return new getVersioningCommand(db)
            .execute()
            .done((versioningConfig: Raven.Client.Server.Versioning.VersioningConfiguration) => {
                this.versioningsLoaded(versioningConfig);
            });
    }

    toDto(): Raven.Client.Server.Versioning.VersioningConfiguration {
        const collectionVersioning = this.versionings();

        const collectionsDto = {} as { [key: string]: Raven.Client.Server.Versioning.VersioningConfigurationCollection; }

        collectionVersioning.forEach(config => {
            collectionsDto[config.collection()] = config.toDto();
        });

        return {
            Default: this.defaultVersioning().toDto(),
            Collections: collectionsDto
        }
    }

    saveChanges() {
        //TODO: check if we have to handle etag here (after Raft is merged)
        let isValid = true;

        this.versionings().forEach(item => {
            if (!this.isValid(item.validationGroup)) {
                isValid = false;
            }
        });

        if (isValid) {
            this.spinners.save(true);

            eventsCollector.default.reportEvent("versioning", "save");

            const dto = this.toDto();
            let disableVersioning = false;

            if (!this.versioningEnabled()) {
                dto.Default.Active = false;
                disableVersioning = true;
            }

            const versioningStatus = disableVersioning ? "disabled" : "enabled";
            new saveVersioningCommand(this.activeDatabase(), dto)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    messagePublisher.reportSuccess(`Versioning has been ${versioningStatus}.`);
                    
                })
                .always(() => this.spinners.save(false));
        }
    }

    createNewVersioning() {
        eventsCollector.default.reportEvent("versioning", "create");

        const newItem = versioningEntry.empty();
        this.versionings.push(newItem);

        // don't show validation errors for newly created items
        newItem.validationGroup.errors.showAllMessages(false);
    }

    removeVersioning(entry: versioningEntry) {
        eventsCollector.default.reportEvent("versioning", "remove");

        this.versionings.remove(entry);
    }

    versioningsLoaded(data: Raven.Client.Server.Versioning.VersioningConfiguration) {
        if (data) {
            this.defaultVersioning(new versioningEntry(versioningEntry.DefaultConfiguration, data.Default));

            const versionings = _.map(data.Collections, (configuration, collection) => {
                return new versioningEntry(collection, configuration);
            });

            this.versionings(versionings);
            this.versioningEnabled(data.Default.Active);
            this.dirtyFlag().reset();
        } else {
            this.versioningEnabled(false);
            this.defaultVersioning(versioningEntry.defaultConfiguration());
        }
    }

    createCollectionNameAutocompleter(item: versioningEntry) {
        return ko.pureComputed(() => {
            const key = item.collection();
            const options = this.collections()
                .filter(x => !x.isAllDocuments && !x.isSystemDocuments && !x.name.startsWith("@"))
                .map(x => x.name);
            const usedOptions = this.versionings().filter(f => f !== item).map(x => x.collection());

            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

}

export = versioning;
