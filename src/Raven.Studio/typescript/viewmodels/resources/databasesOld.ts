
class databases {
    /*
    formatBytes = generalUtils.formatBytesToSize;

    filters = {
        searchText: ko.observable<string>(),
        requestedState: ko.observable<filterState>('all')
    };

    databasesByState: KnockoutComputed<Record<databaseState, number>>;

    spinners = {
        globalToggleDisable: ko.observable<boolean>(false),
        localLockChanges: ko.observableArray<string>([]),
    };

    statsSubscription: changeSubscription;

    databaseNameWidth = ko.observable<number>(350);

    environmentClass = (source: KnockoutObservable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>) => 
        database.createEnvironmentColorComputed("label", source);
   
    databaseToCompact: string;
    popupRestoreDialog: boolean;
    
    constructor() {
        super();
        
        this.initObservables();
    }

    private initObservables() {
        const filters = this.filters;

        filters.searchText.throttle(200).subscribe(() => this.filterDatabases());
        filters.requestedState.subscribe(() => this.filterDatabases());

        
        this.databasesByState = ko.pureComputed(() => {
            const databases = this.databases().sortedDatabases();
            
            const result: Record<databaseState, number> = {
                errored: 0,
                disabled: 0,
                offline: 0,
                online: 0,
                remote: 0
            };

            for (const database of databases) {
                if (database.hasLoadError()) {
                    result.errored++;
                //TODO } else if (!this.isLocalDatabase(database.name)) {
                //TODO:     result.remote++;
                } else if (database.disabled()) {
                    result.disabled++;
                } else if (database.online()) {
                    result.online++;
                } else {
                    result.offline++;
                }
            }
            
            return result;
        });
      
    }


    activate(args: any): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasesInfo> {
        super.activate(args);

        // When coming here from Storage Report View
        if (args && args.compact) {
            this.databaseToCompact = args.compact;
        }

        // When coming here from Backups View, user wants to restore a database
        if (args && args.restore) {
            this.popupRestoreDialog = true;
        }
        
        // we can't use createNotifications here, as it is called after *database changes API* is connected, but user
        // can enter this view and never select database

        this.addNotification(this.changesContext.serverNotifications().watchAllDatabaseChanges((e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) => this.onDatabaseChange(e)));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.fetchDatabases()));
        
        this.registerDisposable(this.changesContext.databaseNotifications.subscribe((dbNotifications) => this.onDatabaseChanged(dbNotifications)));
        
        return this.fetchDatabases();
    }
    
    private onDatabaseChanged(dbChanges: databaseNotificationCenterClient) {
        if (dbChanges) {

            const database = dbChanges.getDatabase();

            const throttledUpdate = _.throttle(() => {
                this.updateDatabaseInfo(database.name);
            }, 10000);
            
            this.statsSubscription = dbChanges.watchAllDatabaseStatsChanged(stats => {
                const matchedDatabase = this.databases().sortedDatabases().find(x => x.name === database.name);
                if (matchedDatabase) {
                    matchedDatabase.documentsCount(stats.CountOfDocuments);
                    matchedDatabase.indexesCount(stats.CountOfIndexes);
                }
                
                // schedule update of other properties
                throttledUpdate();
            });
        } else {
            if (this.statsSubscription) {
                this.statsSubscription.off();
                this.statsSubscription = null;
            }
        }
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        this.updateUrl(appUrl.forDatabases());
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.initTooltips();
        this.setupDisableReasons();
       
        if (this.databaseToCompact) {
            const dbInfo = this.databases().getByName(this.databaseToCompact);
            this.compactDatabase(dbInfo);
        }
        
        if (this.popupRestoreDialog) {
            this.newDatabaseFromBackup();
            this.popupRestoreDialog = false;
        }
    }
    
    deactivate() {
        super.deactivate();
        
        if (this.statsSubscription) {
            this.statsSubscription.off();
            this.statsSubscription = null;
        }
        
        // make we all propovers are hidden
        $('[data-toggle="more-nodes-tooltip"]').popover('hide');
    }

  
    private onDatabaseChange(e: Raven.Server.NotificationCenter.Notifications.Server.DatabaseChanged) {
        switch (e.ChangeType) {
            case "Load":
            case "Put":
            case "Update":
                this.updateDatabaseInfo(e.DatabaseName);
                break;

            case "RemoveNode":
            case "Delete":
                // since we don't know if database was removed from current node, let's fetch databaseInfo first
                this.updateDatabaseInfo(e.DatabaseName)
                    .fail((xhr: JQueryXHR) => {
                        if (xhr.status === 404) {
                            // database was removed from all nodes

                            const db = this.databases().sortedDatabases().find(rs => rs.name === e.DatabaseName);
                            if (db) {
                                this.removeDatabase(db);
                            }
                        }
                    });
                break;
        }
    }

    updateDatabaseInfo(databaseName: string) {
        return new getDatabaseCommand(databaseName)
            .execute()
            .done((result: Raven.Client.ServerWide.Operations.DatabaseInfo) => {
                this.databases().updateDatabase(result);
                this.filterDatabases();
                this.initTooltips();
                this.setupDisableReasons();
            });
    }
    

    private static getLocalStorageKeyForDbNameWidth() {
        return storageKeyProvider.storageKeyFor("databaseNameWidth");
    }
    
    private loadDatabaseNamesSize() {
        return localStorage.getObject(databases.getLocalStorageKeyForDbNameWidth()) || 350;
    }
    
    private saveDatabaseNamesSize(value: number) {
        localStorage.setObject(databases.getLocalStorageKeyForDbNameWidth(), value);
    }
    
    private initTooltips() {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        /* TODO
        const self = this;

        const contentProvider = (dbInfo: databaseInfo) => {
            const nodesPart = dbInfo.nodes().map(node => {
                return `
                <a href="${this.createAllDocumentsUrlObservableForNode(dbInfo, node)()}" 
                    target="${node.tag() === this.clusterManager.localNodeTag() ? "" : "_blank"}" 
                    class="margin-left margin-right ${dbInfo.isBeingDeleted() ? "link-disabled" : ''}" 
                    title="${node.type()}">
                        <i class="${node.cssIcon()}"></i> <span>Node ${node.tag()}</span>
                    </a>
                `
            }).join(" ");
            
            return `<div class="more-nodes-tooltip">
                <div>
                    <i class="icon-dbgroup"></i>
                    <span>
                        Database Group for ${dbInfo.name}
                    </span>
                </div>
                <hr />
                <div class="flex-horizontal flex-wrap">
                    ${nodesPart}    
                </div>
            </div>`;
        };
        


        $('.databases [data-toggle="size-tooltip"]').tooltip({
            container: "body",
            html: true,
            placement: "right",
            title: function () {
                const $data = ko.dataFor(this) as databaseInfo;
                return `<div class="text-left padding padding-sm">
                    Data: <strong>${self.formatBytes($data.totalSize())}</strong><br />
                Temp: <strong>${self.formatBytes($data.totalTempBuffersSize())}</strong><br />
                    Total: <strong>${self.formatBytes($data.totalSize() + $data.totalTempBuffersSize())}</strong>
                </div>`
            }
        });
        
         *
    }
    */

    private filterDatabases(): void {
        /* TODO
        const filters = this.filters;
        let searchText = filters.searchText();
        const hasSearchText = !!searchText;

        if (hasSearchText) {
            searchText = searchText.toLowerCase();
        }

        const matchesFilters = (db: databaseInfo): boolean => {
            const state = filters.requestedState();
            const nodeTag = this.clusterManager.localNodeTag();
            
            const matchesOnline = state === 'online' && db.online();
            const matchesDisabled = state === 'disabled' && db.disabled();
            const matchesErrored = state === 'errored' && db.hasLoadError();
            const matchesOffline = state === 'offline' && (!db.online() && !db.disabled() && !db.hasLoadError() && db.isLocal(nodeTag));
            
            const matchesLocal = state === 'local' && db.isLocal(nodeTag);
            const matchesRemote = state === 'remote' && !db.isLocal(nodeTag);
            const matchesAll = state === 'all';
            
            const matchesText = !hasSearchText || db.name.toLowerCase().indexOf(searchText) >= 0;
            
            return matchesText &&
                (matchesOnline || matchesDisabled || matchesErrored || matchesOffline || matchesLocal || matchesRemote || matchesAll);
        };

        const databases = this.databases();
        databases.sortedDatabases().forEach(db => {
            const matches = matchesFilters(db);
            db.filteredOut(!matches);

            if (!matches) {
                this.selectedDatabases.remove(db.name);
            }
        });
         */
    }

    /*
    createManageDbGroupUrlObsevable(dbInfo: databaseInfo): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            const isLocal = true; //TODO: this.isLocalDatabase(dbInfo.name);
            const link = appUrl.forManageDatabaseGroup(dbInfo);
            if (isLocal) {
                return link;
            } else {
                //TODO: return databases.toExternalUrl(dbInfo, link);
            }
        });
    }

    createAllDocumentsUrlObservable(dbInfo: databaseInfo): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            const isLocal = true; //TOD: this.isLocalDatabase(dbInfo.name);
            const link = appUrl.forDocuments(null, dbInfo);
            if (isLocal) {
                return link;
            } else {
                //TODO: return databases.toExternalUrl(dbInfo, link);
            }
        });
    }
    
    createAllDocumentsUrlObservableForNode(dbInfo: databaseInfo, node: databaseGroupNode) {
        return ko.pureComputed(() => {
            const currentNodeTag = this.clusterManager.localNodeTag();
            const nodeTag = node.tag();
            const link = appUrl.forDocuments(null, dbInfo);
            if (currentNodeTag === nodeTag) {
                return link;
            } else {
                return appUrl.toExternalUrl(node.serverUrl(), link);
            }
        });
    }
    

    indexErrorsUrl(dbInfo: databaseInfo): string {
        return appUrl.forIndexErrors(dbInfo);
    }

    storageReportUrl(dbInfo: databaseInfo): string {
        return appUrl.forStatusStorageReport(dbInfo);
    }

    indexesUrl(dbInfo: databaseInfo): string {
        return appUrl.forIndexes(dbInfo);
    } 

    backupsViewUrl(dbInfo: databaseInfo): string {
        return appUrl.forBackups(dbInfo);
    }

    periodicBackupUrl(dbInfo: databaseInfo): string {
        return appUrl.forEditPeriodicBackupTask(dbInfo);
    }

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

    
    toggleDisableDatabaseIndexing(db: databaseInfo) {
        const enableIndexing = db.indexingDisabled();
        const message = enableIndexing ? "Enable" : "Disable";

        eventsCollector.default.reportEvent("databases", "toggle-indexing");

        this.confirmationMessage("Are you sure?", message + " indexing?")
            .done(result => {
                if (result.can) {
                    db.inProgressAction(enableIndexing ? "Enabling..." : "Disabling...");

                    new toggleDisableIndexingCommand(enableIndexing, { name: db.name })
                        .execute()
                        .done(() => {
                            db.indexingDisabled(!enableIndexing);
                            db.indexingPaused(false);
                        })
                        .always(() => db.inProgressAction(null));
                }
            });
    }
    
  

    togglePauseDatabaseIndexing(db: databaseInfo) {
        eventsCollector.default.reportEvent("databases", "pause-indexing");
        
        const pauseIndexing = db.indexingPaused();
        const message = pauseIndexing ? "Resume" : "Pause";

        this.confirmationMessage("Are you sure?", message + " indexing?")
            .done(result => {
                if (result.can) {
                    db.inProgressAction(pauseIndexing ? "Resuming..." : "Pausing...");

                    new togglePauseIndexingCommand(pauseIndexing, db.asDatabase())
                        .execute()
                        .done(() => db.indexingPaused(!pauseIndexing))
                        .always(() => db.inProgressAction(null));
                }
            });
    }


    databasePanelClicked(dbInfo: databaseInfo, event: JQueryEventObject) {
        if (generalUtils.canConsumeDelegatedEvent(event)) {
            this.activateDatabase(dbInfo);
            return false;
        }
        
        return true;
    }
    
    activateDatabase(dbInfo: databaseInfo) {
        const db = this.databasesManager.getDatabaseByName(dbInfo.name);
        if (!db || db.disabled() || !db.relevant())
            return true;

        this.databasesManager.activate(db);

        this.updateDatabaseInfo(db.name);
        
        return true; // don't prevent default action as we have links inside links
    }
    
    
    openNotificationCenter(dbInfo: databaseInfo) {
        if (!this.activeDatabase() || this.activeDatabase().name !== dbInfo.name) {
            this.activateDatabase(dbInfo);
        }

        this.notificationCenter.showNotifications.toggle();
    }

    isAdminAccessByDbName(dbName: string) {
        return accessManager.default.isAdminByDbName(dbName);
    }
    
     */
}

export = databases;


/* TODO

class databasesInfo {

    sortedDatabases = ko.observableArray<databaseInfo>();

    databasesCount: KnockoutComputed<number>;

    constructor(dto: Raven.Client.ServerWide.Operations.DatabasesInfo) {

        const databases = dto.Databases.map(db => new databaseInfo(db));

        const dbs = [...databases];
        dbs.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));

        this.sortedDatabases(dbs);

        this.initObservables();
    }

    getByName(name: string) {
        return this.sortedDatabases().find(x => x.name.toLowerCase() === name.toLowerCase());
    }

    updateDatabase(newDatabaseInfo: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        const databaseToUpdate = this.getByName(newDatabaseInfo.Name);

        if (databaseToUpdate) {
            databaseToUpdate.update(newDatabaseInfo);
        } else { // new database - create instance of it
            const dto = newDatabaseInfo as Raven.Client.ServerWide.Operations.DatabaseInfo;
            const databaseToAdd = new databaseInfo(dto);
            this.sortedDatabases.push(databaseToAdd);
            this.sortedDatabases.sort((a, b) => generalUtils.sortAlphaNumeric(a.name, b.name));
        }
    }

    private initObservables() {
        this.databasesCount = ko.pureComputed(() => this
            .sortedDatabases()
            .filter(r => r instanceof databaseInfo)
            .length);
    }
}


//TODO: consider removing 
class databaseInfo {

    private static dayAsSeconds = 60 * 60 * 24;

    name: string;

    uptime = ko.observable<string>();
    totalSize = ko.observable<number>();
    totalTempBuffersSize = ko.observable<number>();
    bundles = ko.observableArray<string>();
    backupStatus = ko.observable<string>();
    lastBackupText = ko.observable<string>();
    lastFullOrIncrementalBackup = ko.observable<string>();
    dynamicDatabaseDistribution = ko.observable<boolean>();

    loadError = ko.observable<string>();

    isEncrypted = ko.observable<boolean>();
    isAdmin = ko.observable<boolean>();
    disabled = ko.observable<boolean>();
    lockMode = ko.observable<Raven.Client.ServerWide.DatabaseLockMode>();

    filteredOut = ko.observable<boolean>(false);
    isBeingDeleted = ko.observable<boolean>(false);

    indexingErrors = ko.observable<number>();
    alerts = ko.observable<number>();
    performanceHints = ko.observable<number>();

    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    
    online: KnockoutComputed<boolean>;
    isLoading: KnockoutComputed<boolean>;
    hasLoadError: KnockoutComputed<boolean>;
    canNavigateToDatabase: KnockoutComputed<boolean>;
    isCurrentlyActiveDatabase: KnockoutComputed<boolean>;
    
    databaseAccessText = ko.observable<string>();
    databaseAccessColor = ko.observable<string>();
    databaseAccessClass = ko.observable<string>();

    inProgressAction = ko.observable<string>();

    rejectClients = ko.observable<boolean>();
    indexingStatus = ko.observable<Raven.Client.Documents.Indexes.IndexRunningStatus>();
    indexingDisabled = ko.observable<boolean>();
    indexingPaused = ko.observable<boolean>();
    documentsCount = ko.observable<number>();
    indexesCount = ko.observable<number>();

    private computeBackupStatus(backupInfo: Raven.Client.ServerWide.Operations.BackupInfo) {
        if (!backupInfo || !backupInfo.LastBackup) {
            this.lastBackupText("Never backed up");
            return "text-danger";
        }

        const dateInUtc = moment.utc(backupInfo.LastBackup);
        const diff = moment().utc().diff(dateInUtc);
        const durationInSeconds = moment.duration(diff).asSeconds();

        this.lastBackupText(`Backed up ${this.lastFullOrIncrementalBackup()}`);
        return durationInSeconds > databaseInfo.dayAsSeconds ? "text-warning" : "text-success";
    }
    
    private initializeObservables() {
        this.hasLoadError = ko.pureComputed(() => !!this.loadError());

        this.online = ko.pureComputed(() => {
            return !!this.uptime();
        });

        this.canNavigateToDatabase = ko.pureComputed(() => {
            const enabled = !this.disabled();
            const hasLoadError = this.hasLoadError();
            return enabled && !hasLoadError;
        });

        this.isCurrentlyActiveDatabase = ko.pureComputed(() => {
            const currentDatabase = activeDatabaseTracker.default.database();

            if (!currentDatabase) {
                return false;
            }

            return currentDatabase.name === this.name;
        });

        this.isLoading = ko.pureComputed(() => {
            return this.isCurrentlyActiveDatabase() &&
                !this.online() &&
                !this.disabled();
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.DatabaseInfo): void {
        this.isAdmin(dto.IsAdmin);
        this.totalSize(dto.TotalSize ? dto.TotalSize.SizeInBytes : 0);
        this.totalTempBuffersSize(dto.TempBuffersSize ? dto.TempBuffersSize.SizeInBytes : 0);
        this.loadError(dto.LoadError);
        this.uptime(generalUtils.timeSpanAsAgo(dto.UpTime, false));
        this.dynamicDatabaseDistribution(dto.DynamicNodesDistribution);
        
        this.environment(dto.Environment);

        if (dto.BackupInfo && dto.BackupInfo.LastBackup) {
            this.lastFullOrIncrementalBackup(moment.utc(dto.BackupInfo.LastBackup).local().fromNow());
        }
            
        this.backupStatus(this.computeBackupStatus(dto.BackupInfo));

        this.rejectClients(dto.RejectClients);
        this.indexingStatus(dto.IndexingStatus);
        this.indexingDisabled(dto.IndexingStatus === "Disabled");
        this.indexingPaused(dto.IndexingStatus === "Paused");
        this.deletionInProgress(dto.DeletionInProgress ? Object.keys(dto.DeletionInProgress) : []);
        this.databaseAccessText(accessManager.default.getDatabaseAccessLevelTextByDbName(this.name));
        this.databaseAccessColor(accessManager.default.getAccessColorByDbName(this.name));
        this.databaseAccessClass(accessManager.default.getAccessIconByDbName(this.name))
    }
}

export = databaseInfo;

 */
