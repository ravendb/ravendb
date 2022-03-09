import router = require("plugins/router");
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
import shardedDatabase from "models/resources/shardedDatabase";
import nonShardedDatabase from "models/resources/nonShardedDatabase";
import shard from "models/resources/shard";
import { shardingTodo } from "common/developmentHelper";

class databasesManager {

    static default = new databasesManager();

    initialized = $.Deferred<void>();

    activeDatabaseTracker = activeDatabaseTracker.default;
    changesContext = changesContext.default;

    private databaseToActivate = ko.observable<string>();

    databases = ko.observableArray<database>([]);

    onDatabaseDeletedCallbacks: Array<(qualifier: string, name: string) => void> = [
        (q, n) => recentQueriesStorage.onDatabaseDeleted(q, n),
        (q, n) => starredDocumentsStorage.onDatabaseDeleted(q, n),
        (q, n) => savedPatchesStorage.onDatabaseDeleted(q, n)
    ];

    getDatabaseByName(name: string): database {
        if (!name) {
            return null;
        }
        
        const singleShard = name.includes("$");
        if (singleShard) {
            const groupName = databasesManager.shardGroupKey(name);
            const dbByName = this
                .databases()
                .find(x => groupName.toLowerCase() === x.name.toLowerCase()) as shardedDatabase;
            
            if (dbByName) {
                return dbByName.shards().find(x => x.name.toLowerCase() === name.toLowerCase());
            } else {
                return null;
            }
        } else {
            return this
                .databases()
                .find(x => name.toLowerCase() === x.name.toLowerCase());
        }
    }

    init(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        return this.refreshDatabases()
            .done(() => {
                router.activate();
                this.initialized.resolve();
            });
    }

    activateAfterCreation(databaseName: string) {
        this.databaseToActivate(databaseName);
    }

    refreshDatabases(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        return new getDatabasesCommand()
            .execute()
            .done(result => {
                this.updateDatabases(result);
            });
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
                return this.changesContext.databaseNotifications().connectToWebSocketTask;
            });
        } else {
            return basicTask;
        }
    }

    private static isSharded(data: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        return data.Name.includes("$");
    }
    
    private static shardGroupKey(dbName: string) {
        return dbName.split("$")[0];
    }
    
    private updateDatabases(incomingData: Raven.Client.ServerWide.Operations.DatabasesInfo) {
        this.deleteRemovedDatabases(incomingData);

        const nonSharded = incomingData.Databases.filter(x => !databasesManager.isSharded(x));
        nonSharded.forEach(dbInfo => {
            const existingDb = this.getDatabaseByName(dbInfo.Name);
            this.updateDatabase(dbInfo, existingDb);
        });
        
        const sharded = new Map<string, Raven.Client.ServerWide.Operations.DatabaseInfo[]>();
        incomingData.Databases.forEach(db => {
            if (databasesManager.isSharded(db)) {
                const shardKey = databasesManager.shardGroupKey(db.Name);

                const items = sharded.get(shardKey) || [];
                items.push(db);
                sharded.set(shardKey, items);
            }
        });

        sharded.forEach((shardGroup, shardName) => {
            const existingDb = this.getDatabaseByName(shardName);
            this.updateDatabaseGroup(shardGroup, existingDb);
        });
    }

    private deleteRemovedDatabases(incomingData: Raven.Client.ServerWide.Operations.DatabasesInfo) {
        const existingDatabase = this.databases;

        const toDelete: database[] = [];

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

    private updateDatabaseGroup(dbs: Raven.Client.ServerWide.Operations.DatabaseInfo[], existingDb: database): shardedDatabase {
        if (existingDb) {
            shardingTodo("Marcin"); //TODO:
            //TODO: existingDb.updateUsingGroup(dbs);
            
            return existingDb as shardedDatabase;
        } else {
            const nodeTag = clusterTopologyManager.default.localNodeTag;
            const group = new shardedDatabase(dbs[0], nodeTag);
            shardingTodo("Marcin"); //TODO:
            group.name = databasesManager.shardGroupKey(dbs[0].Name); //TODO: update other props! //TODO: 
            group.shards(dbs.map(db => {
                const singleShard = new shard(db, nodeTag);
                singleShard.parent = group;
                return singleShard;
            }));

            this.databases.push(group);
            this.databases.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));
            
            return group;
        }
    }
    
    private updateDatabase(incomingDatabase: Raven.Client.ServerWide.Operations.DatabaseInfo, matchedExistingRs: database): database {
        if (matchedExistingRs) {
            matchedExistingRs.updateUsing(incomingDatabase);
            return matchedExistingRs;
        } else {
            const newDatabase = new nonShardedDatabase(incomingDatabase, clusterTopologyManager.default.localNodeTag);
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

                const updatedDatabase = this.updateDatabase(rsInfo, this.getDatabaseByName(rsInfo.Name));

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
