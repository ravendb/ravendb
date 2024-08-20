import router = require("plugins/router");
import database = require("models/resources/database");
import changesContext = require("common/changesContext");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import recentQueriesStorage = require("common/storage/savedQueriesStorage");
import starredDocumentsStorage = require("common/storage/starredDocumentsStorage");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import savedPatchesStorage = require("common/storage/savedPatchesStorage");
import generalUtils = require("common/generalUtils");
import mergedIndexesStorage from "common/storage/mergedIndexesStorage";
import shardedDatabase from "models/resources/shardedDatabase";
import nonShardedDatabase from "models/resources/nonShardedDatabase";
import DatabaseUtils from "components/utils/DatabaseUtils";
import getDatabasesForStudioCommand from "commands/resources/getDatabasesForStudioCommand";
import getDatabaseForStudioCommand from "commands/resources/getDatabaseForStudioCommand";
import StudioDatabaseInfo = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabases.StudioDatabaseInfo;
import convertedIndexesToStaticStorage = require("common/storage/convertedIndexesToStaticStorage");

class databasesManager {

    static default = new databasesManager();

    initialized = $.Deferred<void>();

    activeDatabaseTracker = activeDatabaseTracker.default;
    changesContext = changesContext.default;
    
    onUpdateCallback: () => void = () => {
        // empty by default
    };
    
    private databaseToActivate = ko.observable<string>();
    
    databases = ko.observableArray<database>([]);

    //TODO: make sure all those things are saved with root db name!
    onDatabaseDeletedCallbacks: Array<(qualifier: string, name: string) => void> = [
        (q, n) => recentQueriesStorage.onDatabaseDeleted(q, n),
        (q, n) => starredDocumentsStorage.onDatabaseDeleted(q, n),
        (q, n) => savedPatchesStorage.onDatabaseDeleted(q, n),
        (q, n) => mergedIndexesStorage.onDatabaseDeleted(q, n),
        (q, n) => convertedIndexesToStaticStorage.onDatabaseDeleted(q, n),
    ];

    getDatabaseByName(name: string): database {
        if (!name) {
            return null;
        }
        
        const singleShard = DatabaseUtils.isSharded(name);
        if (singleShard) {
            const groupName = DatabaseUtils.shardGroupKey(name);
            const sharded = this.getDatabaseByName(groupName) as shardedDatabase;
            if (sharded) {
                return sharded.shards().find(x => x.name.toLowerCase() === name.toLowerCase());
            }
            return null;
        } else {
            return this
                .databases()
                .find(x => name.toLowerCase() === x.name.toLowerCase());
        }
    }

    init(): JQueryPromise<StudioDatabasesResponse> {
        return this.refreshDatabases()
            .done(() => {
                router.activate();
                this.initialized.resolve();
            });
    }

    activateAfterCreation(databaseName: string) {
        this.databaseToActivate(databaseName);
    }

    refreshDatabases(): JQueryPromise<StudioDatabasesResponse> {
        return new getDatabasesForStudioCommand()
            .execute()
            .done(result => this.updateDatabases(result));
    }

    activateBasedOnCurrentUrl(dbName: string): JQueryPromise<canActivateResultDto> | boolean {
        if (dbName) {
            const db = this.getDatabaseByName(dbName);
            return this.activateIfDifferent(db, dbName);
        }
        
        return true;
    }

    private activateIfDifferent(db: database, dbName: string): JQueryPromise<canActivateResultDto> {
        const task = $.Deferred<canActivateResultDto>();
        const databasesListView = appUrl.forDatabases();

        const incomingDatabaseName = db ? db.name : undefined;

        this.initialized.done(() => {
            const currentActiveDatabase = this.activeDatabaseTracker.database();
            
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
                this.changesContext.databaseNotifications().watchAllDatabaseStatsChanged(e => this.onDatabaseStateUpdateReceivedViaChangesApi(e));
                return this.changesContext.databaseNotifications().connectToWebSocketTask;
            });
        } else {
            this.changesContext.databaseNotifications().watchAllDatabaseStatsChanged(e => this.onDatabaseStateUpdateReceivedViaChangesApi(e));
            return basicTask;
        }
    }

    private updateDatabases(incomingData: StudioDatabasesResponse) {
        this.deleteRemovedDatabases(incomingData);

        incomingData.Databases.forEach(dbDto => {
            const existingDb = this.getDatabaseByName(dbDto.Name);
            this.updateDatabase(dbDto, existingDb);
        });

        this.onUpdateCallback();
    }
    
    private deleteRemovedDatabases(incomingData: StudioDatabasesResponse) {
        const incomingDatabases = incomingData.Databases;
        
        const existingDatabase = this.databases;
        const toDelete: database[] = [];

        this.databases().forEach(db => {
            const matchedDb = incomingDatabases.find(x => x.Name.toLowerCase() === db.name.toLowerCase());
            if (matchedDb) {
                const incomingIsSharded = !!matchedDb.Sharding;
                const localIsSharded = db instanceof shardedDatabase;
                if (incomingIsSharded !== localIsSharded) {
                    // looks like database type changed between requests - put old one on delete list
                    toDelete.push(db);
                }
                
            } else {
                toDelete.push(db);
            }
        });

        existingDatabase.removeAll(toDelete);

        // we also get information about deletion over websocket, so if we are not connected, then notification might be missed, so let's call it here as well
        toDelete.forEach(db => this.onDatabaseDeleted(db));
    }
    
    private updateDatabase(incomingDatabase: StudioDatabaseInfo, matchedExistingRs: database): database {
        if (matchedExistingRs) {
            matchedExistingRs.updateUsing(incomingDatabase);
            return matchedExistingRs;
        } else {
            const sharded = !!incomingDatabase.Sharding;
            
            const localNodeTag = clusterTopologyManager.default.localNodeTag;
            
            const newDatabase = sharded ?
                new shardedDatabase(incomingDatabase, localNodeTag) :
                new nonShardedDatabase(incomingDatabase, localNodeTag);
            
            this.databases.push(newDatabase);
            this.databases.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));
            return newDatabase;
        }
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
                            this.onUpdateCallback();
                        }
                    })
                    .done((info: StudioDatabaseInfo) => {
                        // check if database if still relevant on this node
                        const localTag = clusterTopologyManager.default.localNodeTag();
                        const topologyToUse = info.NodesTopology ?? info.Sharding?.Orchestrator.NodesTopology;
                        const relevant = topologyToUse && (topologyToUse.Members.some(x => x.NodeTag === localTag) ||
                            topologyToUse.Promotables.some(x => x.NodeTag === localTag) ||
                            topologyToUse.Rehabs.some(x => x.NodeTag === localTag));
                        
                        if (!relevant) {
                            this.onNoLongerRelevant(db);
                        }
                    });
                break;
        }
    }

    private onDatabaseStateUpdateReceivedViaChangesApi(event: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) {
        const db = this.getDatabaseByName(event.Database);
        this.updateDatabaseInfo(db, event.Database);
    }

    updateDatabaseInfo(db: database, databaseName: string): JQueryPromise<StudioDatabaseInfo> {
        const rootDatabaseName = DatabaseUtils.shardGroupKey(databaseName);
        
        return new getDatabaseForStudioCommand(rootDatabaseName)
            .execute()
            .done((rsInfo: StudioDatabaseInfo) => {
                if (rsInfo.IsDisabled) {
                    changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
                }

                const updatedDatabase = this.updateDatabase(rsInfo, this.getDatabaseByName(rsInfo.Name));

                this.onUpdateCallback();
                
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
