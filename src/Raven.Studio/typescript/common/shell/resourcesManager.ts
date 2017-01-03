import router = require("plugins/router");
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import timeSeries = require("models/timeSeries/timeSeries");
import resourceActivatedEventArgs = require("viewmodels/resources/resourceActivatedEventArgs");
import changesContext = require("common/changesContext");
import changesApi = require("common/changesApi");
import changeSubscription = require("common/changeSubscription");
import getResourcesCommand = require("commands/resources/getResourcesCommand");
import getResourceCommand = require("commands/resources/getResourceCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import activeResourceTracker = require("common/shell/activeResourceTracker");
import resourceInfo = require("models/resources/info/resourceInfo");

class resourcesManager {

    static default = new resourcesManager();

    initialized = $.Deferred<void>();

    activeResourceTracker = activeResourceTracker.default;
    changesContext = changesContext.default;

    private resourceToActivate = ko.observable<string>();

    resources = ko.observableArray<resource>([]);

    databases = ko.computed<database[]>(() => this.resources().filter(x => x instanceof database) as database[]);
    fileSystems = ko.computed<filesystem[]>(() => this.resources().filter(x => x instanceof filesystem) as filesystem[]);
    counterStorages = ko.computed<counterStorage[]>(() => this.resources().filter(x => x instanceof counterStorage) as counterStorage[]);
    timeSeries = ko.computed<timeSeries[]>(() => this.resources().filter(x => x instanceof timeSeries) as timeSeries[]);

    constructor() { 
        this.changesContext.connectGlobalChangesApi()
            .done(() => {
                this.changesContext.globalChangesApi().watchReconnect(() => this.refreshResources());
            });

        ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (rs: resource) => this.reloadDataAfterReconnection(rs));

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
        if (!name) {
            return null;
        }
        return this
            .databases()
            .find(x => name.toLowerCase() === x.name.toLowerCase());
    }

    getFileSystemByName(name: string): filesystem {
        if (!name) {
            return null;
        }
        return this
            .fileSystems()
            .find(x => name.toLowerCase() === x.name.toLowerCase());
    }

    getCounterStorageByName(name: string): counterStorage {
        if (!name) {
            return null;
        }
        return this
            .counterStorages()
            .find(x => name.toLowerCase() === x.name.toLowerCase());
    }

    getTimeSeriesByName(name: string): timeSeries {
        if (!name) {
            return null;
        }
        return this
            .timeSeries()
            .find(x => name.toLowerCase() === x.name.toLowerCase());
    }

    getResourceByQualifiedName(qualifiedName: string): resource {
        const dbPrefix = database.qualifier + "/";
        const fsPrefix = filesystem.qualifier + "/";
        //TODO: support for cs, ts

        if (qualifiedName.startsWith(dbPrefix)) {
            return this.getDatabaseByName(qualifiedName.substring(dbPrefix.length));
        } else if (qualifiedName.startsWith(fsPrefix)) {
            return this.getFileSystemByName(qualifiedName.substring(fsPrefix.length));
        } else {
            throw new Error("Unable to find resource: " + qualifiedName);
        }
    }

    init(): JQueryPromise<Raven.Client.Data.ResourcesInfo> {
        return this.refreshResources()
            .done(() => {
                this.activateBasedOnCurrentUrl();
                router.activate();
                this.initialized.resolve();
            });
    }

    activateAfterCreation(rsQualifier: string, resourceName: string) {
        this.resourceToActivate(rsQualifier + "/" + resourceName);
    }

    private refreshResources(): JQueryPromise<Raven.Client.Data.ResourcesInfo> {
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

            if (rs && !rs.disabled()) {
                rs.activate(); //TODO: do we need this event right now?
            } else if (rs) {
                messagePublisher.reportError(`${rs.fullTypeName} '${rs.name}' is disabled!`,
                    `You can't access any section of the ${rs.fullTypeName.toLowerCase()} while it's disabled.`);
                router.navigate(urlIfNotFound());
            } else {
                messagePublisher.reportError("The resource " + rsName + " doesn't exist!");
                router.navigate(urlIfNotFound());
            }
        });
    }

    private fetchStudioConfigForDatabase(db: database) {
        //TODO: fetch hot spare and studio config 
    }

    private activateDatabase(db: database) {
        //TODO: this.fecthStudioConfigForDatabase(db);

        this.changesContext.updateChangesApi(db, (changes: changesApi) => {
            /* TODO
            changes.watchAllDocs(() => this.refreshResources()); //TODO: use cooldown - and move to footer?
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

                 /* TODO: redirect to resources page if current resource if no longer available on list

                var activeResource = this.activeResource();
                var actualResourceObservableArray = resourceObservableArray();

                if (!!activeResource && !_.includes(actualResourceObservableArray(), activeResource)) {
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

    private updateResources(incomingData: Raven.Client.Data.ResourcesInfo) {
        this.deleteRemovedResources(incomingData);

        incomingData.Databases.forEach(dbInfo => {
            this.updateResource(dbInfo, name => this.getDatabaseByName(name), database.qualifier);
        });

        /* TODO:
        incomingData.FileSystems.forEach(fsInfo => {
            this.updateResource(fsInfo, name => this.getFileSystemByName(name), filesystem.qualifier);
        });*/
    }

    private deleteRemovedResources(incomingData: Raven.Client.Data.ResourcesInfo) {
        const existingResources = this.resources;

        const toDelete = [] as resource[];

        this.databases().forEach(db => {
            const matchedDb = incomingData.Databases.find(x => x.Name.toLowerCase() === db.name.toLowerCase());
            if (!matchedDb) {
                toDelete.push(db);
            }
        });

        /* TODO
        this.fileSystems().forEach(fs => {
            const matchedFs = incomingData.FileSystems.find(x => x.Name.toLowerCase() === fs.name.toLowerCase());
            if (!matchedFs) {
                toDelete.push(fs);
            }
        });*/

        existingResources.removeAll(toDelete);
    }

    private updateResource(incomingResource: Raven.Client.Data.ResourceInfo, existingResourceFinder: (name: string) => resource, resourceQualifer: string): resource {
        const matchedExistingRs = existingResourceFinder(incomingResource.Name);

        if (matchedExistingRs) {
            matchedExistingRs.updateUsing(incomingResource);
            return matchedExistingRs;
        } else {
            const newResource = this.createResource(resourceQualifer, incomingResource);
            let locationToInsert = _.sortedIndexBy(this.resources(), newResource, (item: resource) => item.qualifiedName);
            this.resources.splice(locationToInsert, 0, newResource);
            return newResource;
        }
    }

    private createResource(qualifer: string, resourceInfo: Raven.Client.Data.ResourceInfo): resource {
        if (database.qualifier === qualifer) {
            return new database(resourceInfo as Raven.Client.Data.DatabaseInfo);
        } else if (filesystem.qualifier === qualifer) {
            return new filesystem(resourceInfo as Raven.Client.Data.FileSystemInfo);
        }

        //TODO: ts, cs
        throw new Error("Unhandled resource type: " + qualifer);
    }

    createGlobalNotifications(): Array<changeSubscription> {
        const globalChanges = changesContext.default.globalChangesApi();

        return [
            globalChanges.watchItemsStartingWith("db/", e => this.onResourceUpdateReceivedViaChangesApi(e)),

            //TODO: fs, cs, ts
             //TODO: DO: this.globalChangesApi.watchDocsStartingWith(shell.studioConfigDocumentId, () => shell.fetchStudioConfig()),*/
        ];
    }

    private onResourceUpdateReceivedViaChangesApi(event: Raven.Server.Alerts.GlobalAlertNotification) {
        let resource = this.getResourceByQualifiedName(event.Id);
        if (event.Operation === "Delete" && resource) {
            changesContext.default.disconnectIfCurrent(resource, "ResourceDeleted");
            this.resources.remove(resource);

        } else if (event.Operation === "Write") {
            const [prefix, name] = event.Id.split("/", 2);
            new getResourceCommand(prefix, name)
                .execute()
                .done((rsInfo: Raven.Client.Data.ResourceInfo) => {

                    if (rsInfo.Disabled) {
                        changesContext.default.disconnectIfCurrent(resource, "ResourceDisabled");
                    }

                    //TODO: it supports db only for now!
                    if (prefix !== database.qualifier) {
                        throw new Error("we support db only for now!"); //TODO: delete me once we support more types
                    }

                    const updatedResource = this.updateResource(rsInfo, name => this.getDatabaseByName(name), database.qualifier);

                    const toActivate = this.resourceToActivate();

                    if (toActivate && toActivate === event.Id) {
                        updatedResource.activate();
                        this.resourceToActivate(null);
                    }
                });
        }
    }
    
    /*TODO
    private activateCounterStorage(cs: counterStorage) {
        var changesSubscriptionArray = () => [
            changesContext.currentResourceChangesApi().watchAllCounters(() => this.fetchCsStats(cs)),
            changesContext.currentResourceChangesApi().watchCounterBulkOperation(() => this.fetchCsStats(cs))
        ];
        var isNotACounterStorage = this.isPreviousDifferentKind(TenantType.CounterStorage);
        this.updateChangesApi(cs, isNotACounterStorage, () => this.fetchCsStats(cs), changesSubscriptionArray);
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
