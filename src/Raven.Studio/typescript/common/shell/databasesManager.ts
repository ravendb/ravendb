import router = require("plugins/router");
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import changesContext = require("common/changesContext");
import changeSubscription = require("common/changeSubscription");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import mergedIndexesStorage = require("common/storage/mergedIndexesStorage");
import recentPatchesStorage = require("common/storage/recentPatchesStorage");
import recentQueriesStorage = require("common/storage/recentQueriesStorage");
import starredDocumentsStorage = require("common/storage/starredDocumentsStorage");

class databasesManager {

    static default = new databasesManager();

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
    }

    getDatabaseByName(name: string): database {
        if (!name) {
            return null;
        }
        return this
            .databases()
            .find(x => name.toLowerCase() === x.name.toLowerCase());
    }

    init(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        return this.refreshDatabases()
            .done(() => {
                this.activateBasedOnCurrentUrl();
                router.activate();
                this.initialized.resolve();
            });
    }

    activateAfterCreation(databaseName: string) {
        this.databaseToActivate(databaseName);
    }

    private refreshDatabases(): JQueryPromise<Raven.Client.Server.Operations.DatabasesInfo> {
        return new getDatabasesCommand()
            .execute()
            .done(result => {
                this.updateDatabases(result);
            });
        //TODO: .fail(result => this.handleRavenConnectionFailure(result))
    }

    activateBasedOnCurrentUrl(): JQueryPromise<canActivateResultDto> | boolean {
        const dbUrl = appUrl.getDatabaseNameFromUrl();
        if (dbUrl) {
            const db = this.getDatabaseByName(dbUrl);
            return this.activateIfDifferent(db, dbUrl, appUrl.forDatabases);
        }
        return true;
    }

    private activateIfDifferent(db: database, dbName: string, urlIfNotFound: () => string): JQueryPromise<canActivateResultDto> {
        const task = $.Deferred<canActivateResultDto>();

        this.initialized.done(() => {
            const currentActiveDatabase = this.activeDatabaseTracker.database();
            if (currentActiveDatabase != null && currentActiveDatabase.name === db.name) {
                task.resolve({ can: true });
            }

            if (db && !db.disabled()) {
                this.activate(db)
                    .done(() => task.resolve({ can: true }))
                    .fail(() => task.reject());

            } else if (db) {
                messagePublisher.reportError(`${db.fullTypeName} '${db.name}' is disabled!`,
                    `You can't access any section of the ${db.fullTypeName.toLowerCase()} while it's disabled.`);
                router.navigate(urlIfNotFound());
            } else {
                messagePublisher.reportError("The database " + dbName + " doesn't exist!");
                router.navigate(urlIfNotFound());
            }
        });

        return task;
    }

    private fetchStudioConfigForDatabase(db: database) {
        //TODO: fetch hot spare and studio config 
    }

    activate(db: database): JQueryPromise<void> {
        this.changesContext.changeDatabase(db);

        return this.activeDatabaseTracker.onActivation(db);
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

                 /* TODO: redirect to resources page if current database if no longer available on list

                var activeDatabase = this.activeDatabase();
                var actualDatabaseObservableArray = databaseObservableArray();

                if (!!activeDatabase && !_.includes(actualDatabaseObservableArray(), activeDatabase)) {
                    if (actualDatabaseObservableArray.length > 0) {
                        databaseObservableArray().first().activate();
                    } else { //if (actualDatabaseObservableArray.length == 0)
                        shell.disconnectFromDatabaseChangesApi();
                        this.activeDatabase(null);
                    }

                    this.navigate(appUrl.forDatabases());
                }
            }*/
    }

    private updateDatabases(incomingData: Raven.Client.Server.Operations.DatabasesInfo) {
        this.deleteRemovedDatabases(incomingData);

        incomingData.Databases.forEach(dbInfo => {
            this.updateDatabase(dbInfo, name => this.getDatabaseByName(name));
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

    private updateDatabase(incomingDatabase: Raven.Client.Server.Operations.DatabaseInfo, existingDatabaseFinder: (name: string) => database): database {
        const matchedExistingRs = existingDatabaseFinder(incomingDatabase.Name);

        if (matchedExistingRs) {
            matchedExistingRs.updateUsing(incomingDatabase);
            return matchedExistingRs;
        } else {
            const newDatabase = this.createDatabase(incomingDatabase);
            let locationToInsert = _.sortedIndexBy(this.databases(), newDatabase, (item: database) => item.name);
            this.databases.splice(locationToInsert, 0, newDatabase);
            return newDatabase;
        }
    }

    private createDatabase(databaseInfo: Raven.Client.Server.Operations.DatabaseInfo): database {
        return new database(databaseInfo);
    }

    // Please remember those notifications are setup before connection to websocket
    setupGlobalNotifications(): Array<changeSubscription> {
        const serverWideClient = changesContext.default.serverNotifications();

        return [
            serverWideClient.watchAllDatabaseChanges(e => this.onDatabaseUpdateReceivedViaChangesApi(e)),
            serverWideClient.watchReconnect(() => this.refreshDatabases())

             //TODO: DO: this.globalChangesApi.watchDocsStartingWith(shell.studioConfigDocumentId, () => shell.fetchStudioConfig()),*/
        ];
    }

    private onDatabaseUpdateReceivedViaChangesApi(event: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        let db = this.getDatabaseByName(event.DatabaseName);
        if (event.ChangeType === "Delete" && db) {
            
            this.onDatabaseDeleted(db);

        } else if (event.ChangeType === "Put") {
            new getDatabaseCommand(event.DatabaseName)
                .execute()
                .done((rsInfo: Raven.Client.Server.Operations.DatabaseInfo) => {

                    if (rsInfo.Disabled) {
                        changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
                    }

                    const updatedDatabase = this.updateDatabase(rsInfo, name => this.getDatabaseByName(name));

                    const toActivate = this.databaseToActivate();

                    if (toActivate && toActivate === event.DatabaseName) {
                        this.activate(updatedDatabase);
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

export = databasesManager;
