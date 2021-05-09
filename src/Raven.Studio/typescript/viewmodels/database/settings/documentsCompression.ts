import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import eventsCollector = require("common/eventsCollector");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getDocumentsCompressionConfigurationCommand = require("commands/database/documents/getDocumentsCompressionConfigurationCommand");
import saveDocumentsCompressionCommand = require("commands/database/documents/saveDocumentsCompressionCommand");

class documentsCompression extends viewModelBase {

    allExistingCollections: KnockoutComputed<Array<string>>;
    collectionsToCompress = ko.observableArray<string>();
    compressRevisions = ko.observable<boolean>();
    
    collectionToAdd = ko.observable<string>();
    canAddAllCollections: KnockoutComputed<boolean>;
    
    isSaveEnabled: KnockoutComputed<boolean>;

    spinners = {
        save: ko.observable<boolean>(false)
    };

    storageReportUrl: KnockoutComputed<string>;
    
    constructor() {
        super();
        this.bindToCurrentInstance("saveChanges", "addCollection", "removeCollection", "addAllCollections", "addWithBlink");
        this.initObservables();
    }
    
    initObservables() {
        this.allExistingCollections = ko.pureComputed(() => collectionsTracker.default.getCollectionNames().filter(x => x !== "@empty" && x !== "@hilo"));
        
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });

        this.dirtyFlag = new ko.DirtyFlag([this.collectionsToCompress, this.compressRevisions]);
        
        this.canAddAllCollections = ko.pureComputed(() => {
           return !!_.difference(this.allExistingCollections(), this.collectionsToCompress()).length;
        });
        
        this.storageReportUrl = ko.pureComputed(() => appUrl.forStatusStorageReport(this.activeDatabase().name));
    }

    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.fetchCompressionConfiguration(this.activeDatabase())
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseRecord(this.activeDatabase()) }));

                return deferred;
            });
    }

    private fetchCompressionConfiguration(db: database): JQueryPromise<Raven.Client.ServerWide.DocumentsCompressionConfiguration> {
        return new getDocumentsCompressionConfigurationCommand(db)
            .execute()
            .done((config: Raven.Client.ServerWide.DocumentsCompressionConfiguration) => {
                this.onConfigurationLoaded(config);
            });
    }

    onConfigurationLoaded(data: Raven.Client.ServerWide.DocumentsCompressionConfiguration) {
        this.compressRevisions(data ? data.CompressRevisions : false);
        this.collectionsToCompress(data ? data.Collections : []);
        this.dirtyFlag().reset();
    }

    createCollectionNameAutocompleter() {
        return ko.pureComputed(() => {
            const key = this.collectionToAdd();
            
            const options = this.allExistingCollections();
            const usedOptions = this.collectionsToCompress();
            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    addCollection() {
        this.addWithBlink(this.collectionToAdd());
    }

    addWithBlink(collectionName: string) {
        if (!this.collectionsToCompress().find(x => x === collectionName)) {
            this.collectionsToCompress.unshift(collectionName);
        }
        
        this.collectionToAdd("");

        $(".collection-list li").first().addClass("blink-style");
    }

    addAllCollections() {
        const collections = _.uniq(this.collectionsToCompress().concat(this.allExistingCollections())).sort();
        this.collectionsToCompress(collections);
    }
    
    removeCollection(collectionName: string) {
        this.collectionsToCompress.remove(collectionName);
    }

    saveChanges() {
        this.spinners.save(true);
        eventsCollector.default.reportEvent("documents-compression", "save");
        const dto = this.toDto();

        new saveDocumentsCompressionCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => this.dirtyFlag().reset())
            .always(() => this.spinners.save(false));
    }

    toDto(): Raven.Client.ServerWide.DocumentsCompressionConfiguration {
        return {
            Collections: this.collectionsToCompress(),
            CompressRevisions: this.compressRevisions()
        }
    }
}

export = documentsCompression
