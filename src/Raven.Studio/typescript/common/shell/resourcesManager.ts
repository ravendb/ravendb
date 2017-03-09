import router = require("plugins/router");
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import databaseActivatedEventArgs = require("viewmodels/resources/databaseActivatedEventArgs");
import changesContext = require("common/changesContext");
import changesApi = require("common/changesApi");
import changeSubscription = require("common/changeSubscription");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import databaseInfo = require("models/resources/info/databaseInfo");
import mergedIndexesStorage = require("common/storage/mergedIndexesStorage");
import recentPatchesStorage = require("common/storage/recentPatchesStorage");
import recentQueriesStorage = require("common/storage/recentQueriesStorage");
import starredDocumentsStorage = require("common/storage/starredDocumentsStorage");

class resourcesManager {

    static default = new resourcesManager();

    initialized = $.Deferred<void>();

    activeDatabaseTracker = activeDatabaseTracker.default;
    changesContext = changesContext.default;

    private databaseToActivate = ko.observable<string>();

    databases = ko.observableArray<database>([]);

    onDatabaseDeletedCallbacks = [
        (q, n) => mergedIndexesStorage.onDatabaseDeleted(q, n),
        (q, n) => recentPatchesStorage.onDatabaseDeleted(q, n),
        (q, n) => recentQueriesStorage.onDatabaseDeleted(q, n),
        (q, n) => starredDocumentsStorage.onDatabaseDeleted(q, n)
    ] as Array<(qualifier: string, name: string) => void>;

    constructor() { 
        ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (db: database) => this.reloadDataAfterReconnection(db));

        ko.postbox.subscribe(EVENTS.Database.Activate, ({ database }: databaseActivatedEventArgs) => {
            return this.activateDatabase(database);
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

    getDatabaseByQualifiedName(qualifiedName: string): database {
        const dbPrefix = database.qualifier + "/";

        if (qualifiedName.startsWith(dbPrefix)) {
            return this.getDatabaseByName(qualifiedName.substring(dbPrefix.length));
        } else {
            throw new Error("Unable to find resource: " + qualifiedName);
        }
    }

    init(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        return this.refreshDatabases()
            .done(() => {
                this.activateBasedOnCurrentUrl();
                router.activate();
                this.initialized.resolve();
            });
    }

    activateAfterCreation(rsQualifier: string, resourceName: string) {
        this.databaseToActivate(rsQualifier + "/" + resourceName);
    }

    private refreshDatabases(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        return new getDatabasesCommand()
            .execute()
            .done(result => {
                this.updateDatabases(result);
            });
        //TODO: .fail(result => this.handleRavenConnectionFailure(result))
    }

    activateBasedOnCurrentUrl() {
        const dbUrl = appUrl.getDatabaseNameFromUrl();
        if (dbUrl) {
            const db = this.getDatabaseByName(dbUrl);
            this.activateIfDifferent(db, dbUrl, appUrl.forDatabases);
        }
    }

    private activateIfDifferent(db: database, dbName: string, urlIfNotFound: () => string) {
        this.initialized.done(() => {
            const currentActiveDatabase = this.activeDatabaseTracker.database();
            if (currentActiveDatabase != null && currentActiveDatabase.qualifiedName === db.qualifiedName) {
                return;
            }

            if (db && !db.disabled()) {
                db.activate(); //TODO: do we need this event right now?
            } else if (db) {
                messagePublisher.reportError(`${db.fullTypeName} '${db.name}' is disabled!`,
                    `You can't access any section of the ${db.fullTypeName.toLowerCase()} while it's disabled.`);
                router.navigate(urlIfNotFound());
            } else {
                messagePublisher.reportError("The database " + dbName + " doesn't exist!");
                router.navigate(urlIfNotFound());
            }
        });
    }

    private fetchStudioConfigForDatabase(db: database) {
        //TODO: fetch hot spare and studio config 
    }

    private activateDatabase(db: database) {
        //TODO: this.fecthStudioConfigForDatabase(db);

        this.changesContext.changeDatabase(db);
    }

    private reloadDataAfterReconnection(db: database) {
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

    private updateDatabases(incomingData: Raven.Client.Server.Operations.DatabasesInfo) {
        this.deleteRemovedDatabases(incomingData);

        incomingData.Databases.forEach(dbInfo => {
            this.updateDatabase(dbInfo, name => this.getDatabaseByName(name), database.qualifier);
        });
    }

    private deleteRemovedDatabases(incomingData: Raven.Client.Server.Operations.DatabasesInfo) {
        const existingDatabase = this.databases;

        const toDelete = [] as database[];

        this.databases().forEach(db => {
            const matchedDb = incomingData.Databases.find(x => x.Name.toLowerCase() === db.name.toLowerCase());
            if (!matchedDb) {
                toDelete.push(db);
            }
        });

        existingDatabase.removeAll(toDelete);
    }

    private updateDatabase(incomingDatabase: Raven.Client.Server.Operations.DatabaseInfo, existingDatabaseFinder: (name: string) => database, resourceQualifer: string): database {
        const matchedExistingRs = existingDatabaseFinder(incomingDatabase.Name);

        if (matchedExistingRs) {
            matchedExistingRs.updateUsing(incomingDatabase);
            return matchedExistingRs;
        } else {
            const newDatabase = this.createDatabase(resourceQualifer, incomingDatabase);
            let locationToInsert = _.sortedIndexBy(this.databases(), newDatabase, (item: database) => item.qualifiedName);
            this.databases.splice(locationToInsert, 0, newDatabase);
            return newDatabase;
        }
    }

    private createDatabase(qualifer: string, databaseInfo: Raven.Client.Server.Operations.DatabaseInfo): database {
        if (database.qualifier === qualifer) {
            return new database(databaseInfo as Raven.Client.Server.Operations.DatabaseInfo);
        }
        throw new Error("Unhandled resource type: " + qualifer);
    }

    // Please remember those notifications are setup before connection to websocket
    setupGlobalNotifications(): Array<changeSubscription> {
        const serverWideClient = changesContext.default.serverNotifications();

        return [
            serverWideClient.watchDatabaseChangeStartingWith("db/", e => this.onDatabaseUpdateReceivedViaChangesApi(e)),
            serverWideClient.watchReconnect(() => this.refreshDatabases())

             //TODO: DO: this.globalChangesApi.watchDocsStartingWith(shell.studioConfigDocumentId, () => shell.fetchStudioConfig()),*/
        ];
    }

    private onDatabaseUpdateReceivedViaChangesApi(event: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        let db = this.getDatabaseByQualifiedName(event.DatabaseName);
        if (event.ChangeType === "Delete" && db) {
            
            this.onDatabaseDeleted(db);

        } else if (event.ChangeType === "Put") {
            const [prefix, name] = event.DatabaseName.split("/", 2);
            new getDatabaseCommand(name)
                .execute()
                .done((rsInfo: Raven.Client.Server.Operations.DatabaseInfo) => {

                    if (rsInfo.Disabled) {
                        changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
                    }

                    const updatedDatabase = this.updateDatabase(rsInfo, name => this.getDatabaseByName(name), database.qualifier);

                    const toActivate = this.databaseToActivate();

                    if (toActivate && toActivate === event.DatabaseName) {
                        updatedDatabase.activate();
                        this.databaseToActivate(null);
                    }
                });
        }
    }

    private onDatabaseDeleted(db: database) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseDeleted");
        this.databases.remove(db);

        this.onDatabaseDeletedCallbacks.forEach(callback => {
            callback(db.qualifier, db.name);
        });
    }
   
}

export = resourcesManager;
