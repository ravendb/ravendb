import router = require("plugins/router");
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import changesContext = require("common/changesContext");
import getDatabasesCommand = require("commands/resources/getDatabasesCommand");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import recentQueriesStorage = require("common/storage/savedQueriesStorage");
import starredDocumentsStorage = require("common/storage/starredDocumentsStorage");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import savedPatchesStorage = require("common/storage/savedPatchesStorage");
import generalUtils = require("common/generalUtils");

class databasesManager {

    static default = new databasesManager();

    initialized = $.Deferred<void>();

    activeDatabaseTracker = activeDatabaseTracker.default;
    changesContext = changesContext.default;

    private databaseToActivate = ko.observable<string>();

    databases = ko.observableArray<database>([]);

    onDatabaseDeletedCallbacks = [
        (q, n) => recentQueriesStorage.onDatabaseDeleted(q, n),
        (q, n) => starredDocumentsStorage.onDatabaseDeleted(q, n),
        (q, n) => savedPatchesStorage.onDatabaseDeleted(q, n)
    ] as Array<(qualifier: string, name: string) => void>;

    getDatabaseByName(name: string): database {
        if (!name) {
            return null;
        }
        return this
            .databases()
            .find(x => name.toLowerCase() === x.name.toLowerCase());
    }

    init(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        return this.refreshDatabases()
            .done(() => {
                const dbNameFromUrl = appUrl.getDatabaseNameFromUrl();
                this.activateBasedOnCurrentUrl(dbNameFromUrl, true);
                router.activate();
                this.initialized.resolve();
            });
    }

    activateAfterCreation(databaseName: string) {
        this.databaseToActivate(databaseName);
    }

    private refreshDatabases(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        return new getDatabasesCommand()
            .execute()
            .done(result => {
                this.updateDatabases(result);
            });
    }

    activateBasedOnCurrentUrl(dbName: string, isViewAllowed: boolean): JQueryPromise<canActivateResultDto> | boolean {
        if (dbName) {
            const db = this.getDatabaseByName(dbName);
            return this.activateIfDifferent(db, dbName, isViewAllowed);
        }
        return true;
    }

    private activateIfDifferent(db: database, dbName: string, isViewAllowed: boolean): JQueryPromise<canActivateResultDto> {
        const task = $.Deferred<canActivateResultDto>();
        const databasesListView = appUrl.forDatabases();

        const incomingDatabaseName = db ? db.name : undefined;

        this.initialized.done(() => {
            const currentActiveDatabase = this.activeDatabaseTracker.database();

            if (!isViewAllowed) {
                messagePublisher.reportError("Redirecting. Invalid access level.");
                task.resolve({ redirect: appUrl.forDatabases() });
                return task;
            }
            
            if (currentActiveDatabase != null && currentActiveDatabase.name === incomingDatabaseName) {
                task.resolve({ can: true });
                return;
            }

            if (db && !db.disabled() && db.relevant()) {
                this.activate(db)
                    .done(() => task.resolve({ can: true }))
                    .fail(() => task.reject());

            } else if (db && db.disabled()) {
                messagePublisher.reportError(`${db.fullTypeName} '${db.name}' is disabled!`,
                    `You can't access any section of the ${db.fullTypeName.toLowerCase()} while it's disabled.`, null, false);
                task.resolve({ redirect: databasesListView });
                return task;
            } else if (db && !db.relevant()) {
                messagePublisher.reportError(`${db.fullTypeName} '${db.name}' is not relevant on this node!`,
                    `You can't access any section of the ${db.fullTypeName.toLowerCase()} while it's not relevant.`, null, false);
                task.resolve({ redirect: databasesListView });
                return task;
            } else {
                messagePublisher.reportError("The database " + dbName + " doesn't exist!", null, null, false);
                task.resolve({ redirect: databasesListView });
                return task;
            }
        });

        return task;
    }

    activate(db: database, opts: { waitForNotificationCenterWebSocket: boolean } = undefined): JQueryPromise<void> {
        this.changesContext.changeDatabase(db);

        const basicTask = this.activeDatabaseTracker.onActivation(db);
        
        const waitForNotificationCenter = opts ? opts.waitForNotificationCenterWebSocket : false;
        
        if (waitForNotificationCenter) {
            return basicTask.then(() => {
                return this.changesContext.databaseNotifications().connectToWebSocketTask;
            });
        } else {
            return basicTask;
        }
    }

    private updateDatabases(incomingData: Raven.Client.ServerWide.Operations.DatabasesInfo) {
        this.deleteRemovedDatabases(incomingData);

        incomingData.Databases.forEach(dbInfo => {
            this.updateDatabase(dbInfo, name => this.getDatabaseByName(name));
        });
    }

    private deleteRemovedDatabases(incomingData: Raven.Client.ServerWide.Operations.DatabasesInfo) {
        const existingDatabase = this.databases;

        const toDelete = [] as database[];

        this.databases().forEach(db => {
            const matchedDb = incomingData.Databases.find(x => x.Name.toLowerCase() === db.name.toLowerCase());
            if (!matchedDb) {
                toDelete.push(db);
            }
        });

        existingDatabase.removeAll(toDelete);

        // we also get information about deletion over websocket, so if we are not connected, then notification might be missed, so let's call it here as well
        toDelete.forEach(db => this.onDatabaseDeleted(db));
    }

    private updateDatabase(incomingDatabase: Raven.Client.ServerWide.Operations.DatabaseInfo, existingDatabaseFinder: (name: string) => database): database {
        const matchedExistingRs = existingDatabaseFinder(incomingDatabase.Name);

        if (matchedExistingRs) {
            matchedExistingRs.updateUsing(incomingDatabase);
            return matchedExistingRs;
        } else {
            const newDatabase = this.createDatabase(incomingDatabase);
            this.databases.push(newDatabase);
            this.databases.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));
            return newDatabase;
        }
    }

    private createDatabase(databaseInfo: Raven.Client.ServerWide.Operations.DatabaseInfo): database {
        return new database(databaseInfo, clusterTopologyManager.default.localNodeTag);
    }

    // Please remember those notifications are setup before connection to websocket
    setupGlobalNotifications(): void {
        const serverWideClient = changesContext.default.serverNotifications();

        serverWideClient.watchAllDatabaseChanges(e => this.onDatabaseUpdateReceivedViaChangesApi(e));
        serverWideClient.watchReconnect(() => this.refreshDatabases());
    }

    private onDatabaseUpdateReceivedViaChangesApi(event: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        const db = this.getDatabaseByName(event.DatabaseName);

        switch (event.ChangeType) {
            case "Load":
            case "Put":
            case "Update":
                this.updateDatabaseInfo(db, event.DatabaseName);
                break;

            case "RemoveNode":
            case "Delete":
                if (!db) {
                    return;
                }

                // fetch latest database info since we don't know at this point if database was removed from current node
                this.updateDatabaseInfo(db, event.DatabaseName)
                    .fail((xhr: JQueryXHR) => {
                        if (xhr.status === 404) {
                            this.onDatabaseDeleted(db);
                        }
                    })
                    .done((info: Raven.Client.ServerWide.Operations.DatabaseInfo) => {
                        // check if database if still relevant on this node
                        const localTag = clusterTopologyManager.default.localNodeTag();

                        const relevant = !!info.NodesTopology.Members.find(x => x.NodeTag === localTag)
                            || !!info.NodesTopology.Promotables.find(x => x.NodeTag === localTag)
                            || !!info.NodesTopology.Rehabs.find(x => x.NodeTag === localTag);

                        if (!relevant) {
                            this.onNoLongerRelevant(db);
                        }
                    });
                break;
        }
    }

    private updateDatabaseInfo(db: database, databaseName: string): JQueryPromise<Raven.Client.ServerWide.Operations.DatabaseInfo> {
        return new getDatabaseCommand(databaseName)
            .execute()
            .done((rsInfo: Raven.Client.ServerWide.Operations.DatabaseInfo) => {

                if (rsInfo.Disabled) {
                    changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
                }

                const updatedDatabase = this.updateDatabase(rsInfo, name => this.getDatabaseByName(name));

                const toActivate = this.databaseToActivate();

                if (updatedDatabase.relevant() && toActivate && toActivate === databaseName) {
                    this.activate(updatedDatabase);
                    this.databaseToActivate(null);
                }
            });
    }

    private onDatabaseDeleted(db: database) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseDeleted");
        this.databases.remove(db);

        this.onDatabaseDeletedCallbacks.forEach(callback => {
            callback(db.qualifier, db.name);
        });
    }
    
    private onNoLongerRelevant(db: database) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseIsNotRelevant");
    }
   
}

export = databasesManager;
