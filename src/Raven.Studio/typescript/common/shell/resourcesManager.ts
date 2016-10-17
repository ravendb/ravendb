import router = require("plugins/router");
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterstorage");
import timeSeries = require("models/timeseries/timeseries");
import resourceActivatedEventArgs = require("viewmodels/resources/resourceActivatedEventArgs");
import changesContext = require("common/changesContext");
import changesApi = require("common/changesApi");
import changeSubscription = require("common/changeSubscription");
import resourcesInfo = require("models/resources/info/resourcesInfo");
import getResourcesCommand = require("commands/resources/getResourcesCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import activeResourceTracker = require("common/shell/activeResourceTracker");

class resourcesManager {

    static default = new resourcesManager();

    initialized = $.Deferred<void>();

    activeResourceTracker = activeResourceTracker.default;
    changesContext = changesContext.default;

    resources = ko.observableArray<resource>([]);

    databases = ko.pureComputed<database[]>(() => this.resources().filter(x => x instanceof database) as database[]);
    fileSystems = ko.pureComputed<filesystem[]>(() => this.resources().filter(x => x instanceof filesystem) as filesystem[]);
    counterStorages = ko.pureComputed<counterStorage[]>(() => this.resources().filter(x => x instanceof counterStorage) as counterStorage[]);
    timeSeries = ko.pureComputed<timeSeries[]>(() => this.resources().filter(x => x instanceof timeSeries) as timeSeries[]);

    constructor() {
        ko.postbox.subscribe("ChangesApiReconnected", (rs: resource) => this.reloadDataAfterReconnection(rs));

        ko.postbox.subscribe(EVENTS.Resource.Activate, ({ resource }: resourceActivatedEventArgs) => {
            if (resource instanceof database) {
                return this.activateDatabase(resource as database);
            } else if (resource instanceof filesystem) {
               return this.activateFileSystem(resource as filesystem);
            } else if (resource instanceof counterStorage) {
               //TODO:return this.activateCounterStorage(resource as counterStorage);
            } else if (resource instanceof timeSeries) {
               //TODO: return this.activateTimeSeries(resource as timeSeries);
            }

            throw new Error(`Invalid resource type ${resource.type}`);
        });
    }

    getDatabaseByName(name: string): database {
        return this
            .resources()
            .find(x => x instanceof database && name.toLowerCase() === x.name.toLowerCase()) as database;
    }

    getFileSystemByName(name: string): filesystem {
        return this
            .resources()
            .find(x => x instanceof filesystem && name.toLowerCase() === x.name.toLowerCase()) as filesystem;
    }

    getCounterStorageByName(name: string): counterStorage {
        return this
            .resources()
            .find(x => x instanceof counterStorage && name.toLowerCase() === x.name.toLowerCase()) as counterStorage;
    }

    getTimeSeriesByName(name: string): timeSeries {
        return this
            .resources()
            .find(x => x instanceof timeSeries && name.toLowerCase() === x.name.toLowerCase()) as timeSeries;
    }

    forceResourcesReload() {
        this.refreshResources();
    }

    init(): JQueryPromise<resourcesInfo> {
        return this.refreshResources()
            .done(() => {
                this.activateBasedOnCurrentUrl();
                router.activate(); //TODO: is it correct place for this?
                this.initialized.resolve();
            });
    }

    private refreshResources(): JQueryPromise<resourcesInfo> {
        return new getResourcesCommand()
            .execute()
            .done(result => {
                this.updateResources(result);
            });
        //TODO: .fail(result => this.handleRavenConnectionFailure(result))
    }

    activateBasedOnCurrentUrl() {
        const dbUrl = appUrl.getDatabaseNameFromUrl();
        const fsUrl = appUrl.getFileSystemNameFromUrl();
        const csUrl = appUrl.getCounterStorageNameFromUrl();
        const tsUrl = appUrl.getTimeSeriesNameFromUrl();
        if (dbUrl) {
            const db = this.getDatabaseByName(dbUrl);
            this.activateIfDifferent(db, dbUrl, appUrl.forResources);
        } else if (fsUrl) {
            const fs = this.getFileSystemByName(fsUrl);
            this.activateIfDifferent(fs, fsUrl, appUrl.forResources);
        } else if (tsUrl) {
            const ts = this.getTimeSeriesByName(tsUrl);
            this.activateIfDifferent(ts, tsUrl, appUrl.forResources);
        } else if (csUrl) {
            const cs = this.getCounterStorageByName(csUrl);
            this.activateIfDifferent(cs, csUrl, appUrl.forResources);
        }
    }

    private activateIfDifferent(rs: resource, rsName: string, urlIfNotFound: () => string) {
        this.initialized.done(() => {
            const currentActiveResource = this.activeResourceTracker.resource();
            if (currentActiveResource != null && currentActiveResource.qualifiedName === rs.qualifiedName) {
                return;
            }

            if (rs) {
                rs.activate(); //TODO: do we need this event right now?
            } else {
                messagePublisher.reportError("The resource " + rsName + " doesn't exist!");
                router.navigate(urlIfNotFound());
            }
        });
    }

    private fecthStudioConfigForDatabase(db: database) {
        //TODO: fetch hot spare and studio config 
    }

    private activateDatabase(db: database) {
        //TODO: this.fecthStudioConfigForDatabase(db);

        this.changesContext.updateChangesApi(db, (changes: changesApi) => {
            changes.watchAllDocs(() => this.refreshResources()) //TODO: use cooldown
            /* TODO
            changes.watchAllIndexes(() => this.fetchDbStats(db)),
            changes.watchBulks(() => this.fetchDbStats(db))*/

            return [] as changeSubscription[];
        });
    }

    private activateFileSystem(fs: filesystem) {
        //TODO: ???? this.fecthStudioConfigForDatabase(new database(fs.name));

        this.changesContext.updateChangesApi(fs, (changes: changesApi) => {
            //TODO: watchFsFolders("", () => this.fetchFsStats(fs))
            return [] as changeSubscription[];
        });
    }


    private reloadDataAfterReconnection(rs: resource) {
        /* TODO:
        shell.fetchStudioConfig();
        this.fetchServerBuildVersion();
        this.fetchClientBuildVersion();
        shell.fetchLicenseStatus();
        this.fetchSupportCoverage();
        this.loadServerConfig();
        this.fetchClusterTopology();
        */

        this.refreshResources();
                 /* TODO: redirect to resources page if current resource if no longer available on list

                var activeResource = this.activeResource();
                var actualResourceObservableArray = resourceObservableArray();

                if (!!activeResource && actualResourceObservableArray.contains(activeResource) === false) {
                    if (actualResourceObservableArray.length > 0) {
                        resourceObservableArray().first().activate();
                    } else { //if (actualResourceObservableArray.length == 0)
                        shell.disconnectFromResourceChangesApi();
                        this.activeResource(null);
                    }

                    this.navigate(appUrl.forResources());
                }
            }*/
    }

    private updateResources(incomingData: resourcesInfo) {
        const existingResources = this.resources;
        const incomingResources = incomingData.sortedResources().map(x => x.asResource());
        const incomingResourcesQualified = incomingResources.map(x => x.qualifiedName);

        const deletedResources = existingResources().filter(x => !incomingResourcesQualified.contains(x.qualifier));

        existingResources.removeAll(deletedResources);

        incomingResources.forEach(incomingResource => {
            const matchedExistingRs = existingResources().find(rs => rs.qualifiedName === incomingResource.qualifiedName);

            if (matchedExistingRs) {
                matchedExistingRs.updateUsing(incomingResource);
            } else {
                existingResources.push(incomingResource); //TODO: it isn't sorted
            }
        });
    }


    createNotifications(): Array<changeSubscription> {
        return [
            /* TODO: extract to resources manager class 
            this.globalChangesApi.watchDocsStartingWith("Raven/Databases/", (e) => this.changesApiFiredForResource(e, shell.databases, this.activeDatabase, TenantType.Database)),
            this.globalChangesApi.watchDocsStartingWith("Raven/FileSystems/", (e) => this.changesApiFiredForResource(e, shell.fileSystems, this.activeFilesystem, TenantType.FileSystem)),
            this.globalChangesApi.watchDocsStartingWith("Raven/Counters/", (e) => this.changesApiFiredForResource(e, shell.counterStorages, this.activeCounterStorage, TenantType.CounterStorage)),
            this.globalChangesApi.watchDocsStartingWith("Raven/TimeSeries/", (e) => this.changesApiFiredForResource(e, shell.timeSeries, this.activeTimeSeries, TenantType.TimeSeries)),
            //TODO: this.globalChangesApi.watchDocsStartingWith(shell.studioConfigDocumentId, () => shell.fetchStudioConfig()),*/
        ];
    }

    private onResourceUpdateReceivedViaChangesApi() {
        //TODO: do we have to filter received notifications?
        this.refreshResources();
    }

    /* TODO: 
    private changesApiFiredForResource(e: Raven.Abstractions.Data.DocumentChangeNotification,
        resourceObservableArray: KnockoutObservableArray<any>, activeResourceObservable: any, resourceType: TenantType) {

        if (!!e.Key && (e.Type === "Delete" || e.Type === "Put")) {
            var receivedResourceName = e.Key.slice(e.Key.lastIndexOf('/') + 1);

            if (e.Type === "Delete") {
                var resourceToDelete = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);
                if (!!resourceToDelete) {
                    resourceObservableArray.remove(resourceToDelete);

                    //this.selectNewActiveResourceIfNeeded(resourceObservableArray, activeResourceObservable);
                    if (resourceType == TenantType.Database)
                        recentQueriesStorage.removeRecentQueries(resourceToDelete);
                }
            } else { // e.Type === "Put"
                var getSystemDocumentTask = new getSystemDocumentCommand(e.Key).execute();
                getSystemDocumentTask.done((dto: databaseDocumentDto) => {
                    var existingResource = resourceObservableArray.first((rs: resource) => rs.name == receivedResourceName);

                    if (existingResource == null) { // new resource
                        existingResource = this.createNewResource(resourceType, receivedResourceName, dto);
                        resourceObservableArray.unshift(existingResource);
                    } else {
                        if (existingResource.disabled() != dto.Disabled) { //disable status change
                            existingResource.disabled(dto.Disabled);
                            if (dto.Disabled === false && this.currentConnectedResource.name === receivedResourceName) {
                                existingResource.activate();
                            }
                        }
                    }

                    if (resourceType === TenantType.Database) { //for databases, bundle change or indexing change
                        var dtoSettings: any = dto.Settings;
                        var bundles = !!dtoSettings["Raven/ActiveBundles"] ? dtoSettings["Raven/ActiveBundles"].split(";") : [];
                        existingResource.activeBundles(bundles);


                        var indexingDisabled = this.getIndexingDisbaledValue(dtoSettings["Raven/IndexingDisabled"]);
                        existingResource.indexingDisabled(indexingDisabled);

                        var isRejectclientsEnabled = this.getIndexingDisbaledValue(dtoSettings["Raven/RejectClientsModeEnabled"]);
                        existingResource.rejectClientsMode(isRejectclientsEnabled);
                    }
                });
            }
        }
    }*/


    /*TODO
   
    private fetchFsStats(fs: fileSystem) {
        if (!!fs && !fs.disabled() && fs.isLicensed()) {
            new getFileSystemStatsCommand(fs, true)
                .execute()
                //TODO: .done((result: filesystemStatisticsDto) => fs.saveStatistics(result))
                .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get file system stats", response.responseText, response.statusText));
        }
    }

    private activateCounterStorage(cs: counterStorage) {
        var changesSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchAllCounters(() => this.fetchCsStats(cs)),
            changesContext.currentResourceChangesApi().watchCounterBulkOperation(() => this.fetchCsStats(cs))
        ];
        var isNotACounterStorage = this.isPreviousDifferentKind(TenantType.CounterStorage);
        this.updateChangesApi(cs, isNotACounterStorage, () => this.fetchCsStats(cs), changesSubscriptionArray);
    }

    private fetchCsStats(cs: counterStorage) {
        if (!!cs && !cs.disabled() && cs.isLicensed()) {
            new getCounterStorageStatsCommand(cs, true)
                .execute()
                .done((result: counterStorageStatisticsDto) => cs.saveStatistics(result));
        }
    }

    private activateTimeSeries(ts: timeSeries) {
        var changesSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchAllTimeSeries(() => this.fetchTsStats(ts)),
            changesContext.currentResourceChangesApi().watchTimeSeriesBulkOperation(() => this.fetchTsStats(ts))
        ];
        var isNotATimeSeries = this.isPreviousDifferentKind(TenantType.TimeSeries);
        this.updateChangesApi(ts, isNotATimeSeries, () => this.fetchTsStats(ts), changesSubscriptionArray);
    }*/

   
}

export = resourcesManager;