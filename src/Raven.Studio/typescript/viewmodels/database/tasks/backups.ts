import appUrl = require("common/appUrl");
import router = require("plugins/router");
import database = require("models/resources/database");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import databaseGroupGraph = require("models/database/dbGroup/databaseGroupGraph");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import manualBackupListModel = require("models/database/tasks/manualBackupListModel");
import accessManager = require("common/shell/accessManager");
import getManualBackupCommand = require("commands/database/tasks/getManualBackupCommand");
import shardViewModelBase from "viewmodels/shardViewModelBase";

class backups extends shardViewModelBase {

    view = require("views/database/tasks/backups.html");
    legendView = require("views/partial/databaseGroupLegend.html");
    
    private clusterManager = clusterTopologyManager.default;
    myNodeTag = ko.observable<string>();
    
    recentManualBackup = ko.observable<manualBackupListModel>();
    
    isManualBackupInProgress = ko.observable<boolean>(false); // todo !!! create issue... for server ep

    canNavigateToServerWideBackupTasks: KnockoutComputed<boolean>;
    serverWideTasksUrl: string;
    ongoingTasksUrl: string;

    private graph = new databaseGroupGraph();
    backupsOnly = true; // used in graph legend

    private watchedBackups = new Map<number, number>();
    
    constructor(db: database) {
        super(db);
        this.initObservables();
    }

    private initObservables() {
        this.myNodeTag(this.clusterManager.localNodeTag());
        this.serverWideTasksUrl = appUrl.forServerWideTasks();
        this.ongoingTasksUrl = appUrl.forOngoingTasks(this.db);
        this.canNavigateToServerWideBackupTasks = accessManager.default.isClusterAdminOrClusterNode;
    }

    activate(args: any): JQueryPromise<any> {
        super.activate(args);
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchManualBackup());
    }

    attached() {
        super.attached();

        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        
        this.addNotification(this.changesContext.serverNotifications()
            .watchDatabaseChange(this.db?.name, () => this.refresh()));
        
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));
        
        //this.updateUrl(appUrl.forBackups(this.db));
    }

    compositionComplete(): void {
        super.compositionComplete();

        this.registerDisposableHandler($(document), "fullscreenchange", () => {
            $("body").toggleClass("fullscreen", $(document).fullScreen());
            this.graph.onResize();
        });
        
        this.graph.init($("#databaseGroupGraphContainer"));
    }

    createResponsibleNodeUrl(task: ongoingTaskListModel) {
        return ko.pureComputed(() => {
            const node = task.responsibleNode();
            const db = this.db;
            
            if (node && db) {
                return node.NodeUrl + appUrl.forOngoingTasks(db);
            }
            
            return "#";
        });
    }
    
    private refresh() {
        if (!this.db) {
            return;
        }
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchManualBackup());
    }
    
    private fetchDatabaseInfo() {
        return new getDatabaseCommand(this.db.name)
            .execute()
            .done(dbInfo => {
                this.graph.onDatabaseInfoChanged(dbInfo);
            });
    }
    
    private fetchManualBackup(): JQueryPromise<Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult> {
        const db = this.db.name;
        return new getManualBackupCommand(db)
            .execute()
            .done((manualBackupInfo) => {
                this.processManualBackupResult(manualBackupInfo);
            });
    }

    refreshManualBackupInfo() {
        this.fetchManualBackup();
    }
    
    toggleDetails(item: ongoingTaskListModel) {
        item.toggleDetails();
    }
    
    private processManualBackupResult(dto: Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult) {
       this.recentManualBackup(dto.Status ? new manualBackupListModel(dto.Status) : null);
    }

    createManualBackup() {
        const url = appUrl.forEditManualBackup(this.db);
        router.navigate(url);
    }

    navigateToRestoreDatabase() {
        const url = appUrl.forDatabases("restore");
        router.navigate(url);
    }
}

export = backups;
