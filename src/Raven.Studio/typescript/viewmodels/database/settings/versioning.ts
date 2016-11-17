import viewModelBase = require("viewmodels/viewModelBase");
import versioningEntry = require("models/database/documents/versioningEntry");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import getVersioningsCommand = require("commands/database/documents/getVersioningsCommand");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import document = require("models/database/documents/document");
import eventsCollector = require("common/eventsCollector");

class versioning extends viewModelBase {
    //TODO: introduce model!
    versionings = ko.observableArray<versioningEntry>();
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            this.fetchVersioningEntries(db)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("1UZ5WL");

        this.dirtyFlag = new ko.DirtyFlag([this.versionings]);
        this.isSaveEnabled = ko.computed<boolean>(() => this.dirtyFlag().isDirty());
    }

    private fetchVersioningEntries(db: database): JQueryPromise<Raven.Server.Documents.Versioning.VersioningConfiguration> {
        return new getVersioningsCommand(db).execute()
            .done((versionings: Raven.Server.Documents.Versioning.VersioningConfiguration) => this.versioningsLoaded(versionings));
    }


    toDto(): Raven.Server.Documents.Versioning.VersioningConfiguration {
        const defaultConfiguration = this.versionings().find(x => x.collection() === versioningEntry.DefaultConfiguration);

        const nonDefaultConfiguration = this.versionings().filter(x => x !== defaultConfiguration);

        const collectionsDto = {} as { [key: string]: Raven.Server.Documents.Versioning.VersioningConfigurationCollection; }

        nonDefaultConfiguration.forEach(config => {
            collectionsDto[config.collection()] = config.toDto();
        });

        return {
            Default: defaultConfiguration.toDto(),
            Collections: collectionsDto
        }
    }

    saveChanges() {
        //TODO: check etag
        eventsCollector.default.reportEvent("versioning", "save");

        const dto = this.toDto();
        const versioningDocument = new document(dto);

        new saveDocumentCommand("Raven/Versioning/Configuration", versioningDocument, this.activeDatabase())
            .execute()
            .done((saveResult: saveDocumentResponseDto) => this.versioningsSaved(saveResult));
    }

    createNewVersioning() {
        eventsCollector.default.reportEvent("versioning", "create");

        const emptyVersioning = versioningEntry.empty();

        if (this.versionings().length === 0) {
            emptyVersioning.collection(versioningEntry.DefaultConfiguration);
        }

        this.versionings.push(emptyVersioning);
    }

    removeVersioning(entry: versioningEntry) {
        eventsCollector.default.reportEvent("versioning", "remove");

        this.versionings.remove(entry);
    }

    versioningsLoaded(data: Raven.Server.Documents.Versioning.VersioningConfiguration) {
        if (data) {
            const versionings = [] as Array<versioningEntry>;
            versionings.push(new versioningEntry(versioningEntry.DefaultConfiguration, data.Default));

            for (let collection in data.Collections) {
                const configuration = data.Collections[collection];
                versionings.push(new versioningEntry(collection, configuration));
            }

            this.versionings(versionings);
            this.dirtyFlag().reset();
        }
    }

    versioningsSaved(saveResult: saveDocumentResponseDto) {
        //TODO: test if we have to update etag in metadata to allow subsequent saves
        this.dirtyFlag().reset();
    }

}

export = versioning;
