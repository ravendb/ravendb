import viewModelBase from "viewmodels/viewModelBase";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import getBackupHistoryCommand from "commands/database/tasks/getBackupHistoryCommand";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import generalUtils from "common/generalUtils";
import actionColumn from "widgets/virtualGrid/columns/actionColumn";
import columnPreviewPlugin from "widgets/virtualGrid/columnPreviewPlugin";
import moment from "moment";
import getBackupHistoryDetailsCommand from "commands/database/tasks/getBackupHistoryDetailsCommand";
import operation from "common/notifications/models/operation";
import smugglerDatabaseDetails
    from "viewmodels/common/notificationCenter/detailViewer/operations/smugglerDatabaseDetails";
import notificationCenter from "common/notifications/notificationCenter";
import BackupResult = Raven.Client.Documents.Operations.Backups.BackupResult;
import OperationChanged = Raven.Server.NotificationCenter.Notifications.OperationChanged;
import database from "models/resources/database";
import hyperlinkColumn from "widgets/virtualGrid/columns/hyperlinkColumn";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import appUrl from "common/appUrl";

class backupHistory extends viewModelBase {

    view = require("views/database/status/backupHistory.html");

    static dateTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";

    private gridController = ko.observable<virtualGridController<BackupHistoryItem>>();
    private columnPreview = new columnPreviewPlugin<BackupHistoryItem>();

    backupsViewUrl = this.appUrls.backupsUrl;
  
    private fetchHistory(): JQueryPromise<pagedResult<BackupHistoryItem>> {
        return this.loadBackupHistory()
            .then(history => {
                const flattenedItems: BackupHistoryItem[] = [];
                history.BackupHistory.forEach(historyItem => {
                    flattenedItems.push(historyItem.FullBackup);
                    flattenedItems.push(...historyItem.IncrementalBackups);
                });

               
                return {
                    items: flattenedItems,
                    totalResultCount: flattenedItems.length
                }
            });
    }
    
    private static formatBackupName(data: BackupHistoryItem) {
        const tokens: string[] = [];
        tokens.push(data.TaskId ? "Automatic" : "Manual");
        tokens.push(data.IsFull ? "Full" : "Incremental");
        tokens.push(data.BackupType);
        
        return tokens.join(" ");
    }
    
    compositionComplete() {
        super.compositionComplete();

        const localNodeTag = clusterTopologyManager.default.localNodeTag();
        
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(() => this.fetchHistory(), () =>
            [
                new actionColumn<BackupHistoryItem>(
                    grid,
                    x => this.showDetailsFor(x),
                    "Show",
                    `<i class="icon-preview"></i>`,
                    "72px", {
                        title: () => 'Show backup details',
                        extraClass: item => item.NodeTag !== localNodeTag ? "invisible" : "",
                    }),
                new textColumn<BackupHistoryItem>(grid,
                    x => backupHistory.formatBackupName(x),
                    "Type", "18%", {
                        sortable: "string"
                    }),
                new hyperlinkColumn<BackupHistoryItem>(grid,
                    x => x.TaskName,
                    x => this.getLinkForTaskView(x),
                    "Task Name", "18%", {
                        sortable: "string"
                    }),
                new textColumn<BackupHistoryItem>(grid,
                    x => generalUtils.formatUtcDateAsLocal(x.CreatedAt, backupHistory.dateTimeFormat),
                    "Timestamp", "12%", {
                        sortable: "string"
                    }),
                new textColumn<BackupHistoryItem>(grid,
                    x => x.DurationInMs,
                    "Duration", "12%", {
                        sortable: "string"
                    }),
                new textColumn<BackupHistoryItem>(grid,
                    x => x.NodeTag,
                    "Node", "12%", {
                        sortable: "string"
                    }),
                new textColumn<BackupHistoryItem>(grid,
                    x => x.Error ?? "-",
                    "Error", "12%", {
                        sortable: "string"
                    }),
            ]
        );
        
        grid.setDefaultSortBy(3, "desc");

        this.columnPreview.install("virtual-grid", ".js-backup-history-tooltip",
            (item: BackupHistoryItem, column: textColumn<BackupHistoryItem>,
             e: JQueryEventObject, onValue: (value: any, valueToCopy?: string, wrapValue?: boolean) => void) => {
                if (column.header === "Timestamp") {
                    onValue(moment.utc(item.CreatedAt), item.CreatedAt);
                } else if (column.header === "Duration") {
                    onValue(generalUtils.formatMillis(item.DurationInMs), item.DurationInMs.toLocaleString());
                }
            });
    }
    
    private getLinkForTaskView(item: BackupHistoryItem) {
        if (!item.TaskId) {
            // manual backup
            return null;
        }
        
        return appUrl.forEditPeriodicBackupTask(this.activeDatabase(), item.TaskId);
    }
    
    private createVirtualOperation(item: BackupHistoryItem, result: BackupResult, db: database): OperationChanged {
        return {
            CreatedAt: item.CreatedAt,
            TaskType: "DatabaseExport",
            StartTime: item.CreatedAt,
            EndTime: moment.utc(item.CreatedAt).add(item.DurationInMs, "milliseconds").toISOString(),
            OperationId: -1,
            Killable: false,
            Database: db.name,
            Id: "BackupHistory/" + item.Id,
            DetailedDescription: null,
            Message: "Backup History for: " + db.name,
            IsPersistent: true,
            Title: "Backup History",
            Type: "OperationChanged",
            Severity: "Success",
            State: {
                Result: result,
                Status: "Completed",
                Progress: null
            }
        }
    }
    
    showDetailsFor(item: BackupHistoryItem) {
        const db = this.activeDatabase();
        new getBackupHistoryDetailsCommand(db, item.TaskId, item.Id)
            .execute()
            .done(result => {
                const backupOperation = new operation(db, this.createVirtualOperation(item, result.Details, db));
                smugglerDatabaseDetails.showDetailsFor(backupOperation, notificationCenter.instance);
            });
    }
    
    refresh() {
        this.gridController().reset(false);
    }

    private loadBackupHistory(): JQueryPromise<BackupHistoryResponse> {
        return new getBackupHistoryCommand(this.activeDatabase()).execute();
    }
}

export = backupHistory;
