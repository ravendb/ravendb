import viewModelBase = require("viewmodels/viewModelBase");
import getClusterLogCommand from "commands/database/cluster/getClusterLogCommand";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import columnPreviewPlugin from "widgets/virtualGrid/columnPreviewPlugin";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import actionColumn from "widgets/virtualGrid/columns/actionColumn";
import FollowerDebugView = Raven.Server.Rachis.FollowerDebugView;
import appUrl from "common/appUrl";
import removeEntryFromLogCommand from "commands/database/cluster/removeEntryFromLogCommand";
import getClusterLogEntryCommand from "commands/database/cluster/getClusterLogEntryCommand";
import showDataDialog from "viewmodels/common/showDataDialog";
import app from "durandal/app";
import debugAdvancedClusterSnapshotInstallation from "viewmodels/manage/debugAdvancedClusterSnapshotInstallation";
import notificationCenter from "common/notifications/notificationCenter";
import messagePublisher from "common/messagePublisher";

type LogEntryStatus = "Commited" | "Appended";

interface LogEntry {
    Status: LogEntryStatus;
    CommandType: string;
    CreateAt?: string;
    Flags: Raven.Server.Rachis.RachisEntryFlags;
    Index: number;
    SizeInBytes: number;
    Term: number;
}

class clusterDebug extends viewModelBase {

    view = require("views/manage/debugAdvancedClusterDebug.html");
    
    spinners = {
        refresh: ko.observable<boolean>(false)
    }
  
    clusterLog = ko.observable<Raven.Server.Rachis.RaftDebugView>();
    
    private gridController = ko.observable<virtualGridController<LogEntry>>();
    private columnPreview = new columnPreviewPlugin<LogEntry>();
    
    lastAppendedAsAgo: KnockoutComputed<string>;
    lastCommittedAsAgo: KnockoutComputed<string>;
    installingSnapshot: KnockoutComputed<boolean>;
    progress: KnockoutComputed<number>;
    progressTooltip: KnockoutComputed<string>;
    queueLength: KnockoutComputed<number>;
    rawJsonUrl: KnockoutComputed<string>;
    hasCriticalError: KnockoutComputed<boolean>;
    connections: KnockoutComputed<Raven.Server.Rachis.RaftDebugView.PeerConnection[]>;
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("refresh", "customInlinePreview", "deleteLogEntry", "openInstallationDetails", "openCriticalError", "showConnectionDetails");
        
        this.connections = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log) {
                return [];
            }
            
            if ("ConnectionToPeers" in log) {
                return (log as any).ConnectionToPeers;
            }

            if ("ConnectionToLeader" in log) {
                return [(log as any).ConnectionToLeader];
            }
            
            return [];
        })
        
        this.hasCriticalError = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log) {
                return false;
            }
            
            return !!log.Log.CriticalError;
        });
        
        this.lastAppendedAsAgo = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log) {
                return null;
            }
            
            const date = log.Log.LastAppendedTime;
            if (!date) {
                return null;
            }
            
            return generalUtils.formatDurationByDate(moment.utc(date), true);
        });

        this.lastCommittedAsAgo = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log) {
                return null;
            }

            const date = log.Log.LastCommitedTime;
            if (!date) {
                return null;
            }

            return generalUtils.formatDurationByDate(moment.utc(date), true);
        });
        
        this.installingSnapshot = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log) {
                return false;
            }
            
            return log.Role === "Follower" && (log as FollowerDebugView).Phase === "Snapshot";
        });
        
        this.queueLength = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log || log.Log.Logs.length === 0) {
                return 0;
            }
            
            return log.Log.LastLogEntryIndex - log.Log.CommitIndex;
        });
        
        this.progress = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log) {
                return null;
            }
            
            const first = log.Log.FirstEntryIndex;
            const last = log.Log.LastLogEntryIndex;

            if (!first && !last) {
                return 0;
            }
            
            const logLength = last - first + 1;
            const queueLength = this.queueLength();
            
            return Math.ceil(100 * (logLength - queueLength) / logLength);
        });
        
        this.progressTooltip = ko.pureComputed(() => {
            const log = this.clusterLog();
            if (!log) {
                return null;
            }
            
            return `
                <div>
                    First entry index: <strong>${log.Log.FirstEntryIndex.toLocaleString()}</strong><br />
                    Commit index: <strong>${log.Log.CommitIndex.toLocaleString()}</strong><br />
                    Last log entry index: <strong>${log.Log.LastLogEntryIndex.toLocaleString()}</strong> 
                </div>
              `;
        });

        this.rawJsonUrl = ko.pureComputed(() => appUrl.forAdminClusterLogRawData());
    }
    
    activate(args: any, parameters?: any) {
        super.activate(args, parameters);
        
        return this.fetchClusterLog();
    }

    private static mapStatus(entry: Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry, commitIndex: number): LogEntryStatus {
        const committed = entry.Index <= commitIndex;
        return committed ? "Commited" : "Appended";
    }
    
    compositionComplete(): void {
        super.compositionComplete();
        
        const fetcher = () => {
            const log = this.clusterLog().Log;
            const data = log.Logs;

            return $.when({
                totalResultCount: data.length,
                items: data.map(x => {
                    return {
                        ...x,
                        Status: clusterDebug.mapStatus(x, log.CommitIndex)
                    }
                })
            } as pagedResult<LogEntry>);
        };

        const previewColumn = new actionColumn<LogEntry>(this.gridController(),
            log => this.customInlinePreview(log), "Preview", `<i class="icon-preview"></i>`, "75px",
            {
                title: () => 'Show item preview',
            });

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.setDefaultSortBy(1, "desc");
        grid.init(fetcher, () =>
            [
                previewColumn,
                new textColumn<LogEntry>(grid, x => x.Index, "Index", "15%", {
                    sortable: "number"
                }),
                new textColumn<LogEntry>(grid, x => x.CommandType, "CommandType", "15%", {
                    sortable: "string"
                }),
                new textColumn<LogEntry>(grid, x => x.CreateAt, "Created", "15%", {
                    sortable: "string"
                }),
                new textColumn<LogEntry>(grid, x => generalUtils.formatBytesToSize(x.SizeInBytes), "Size", "15%", {
                    sortable: "number"
                }),
                new textColumn<LogEntry>(grid, x => x.Term, "Term", "15%", {
                    sortable: "number"
                }),
                new textColumn<LogEntry>(grid, x => x.Status, "Status", "15%", {
                    sortable: "string"
                }),
                new actionColumn<LogEntry>(this.gridController(), x => this.deleteLogEntry(x),
                    "Delete",
                    `<i class="icon-trash"></i>`,
                    "35px",
                    {
                        extraClass: () => 'file-trash btn-danger',
                        title: () => 'Delete Log Entry',
                    })
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-cluster-log-tooltip",
            (entry: LogEntry,
             column: textColumn<LogEntry>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                if (column.header === "Created") {
                    onValue(moment.utc(entry.CreateAt), entry.CreateAt);
                } else {
                    const value = column.getCellValue(entry);
                    onValue(generalUtils.escapeHtml(value), value);
                }
               
            });
    }

    deleteLogEntry(log: LogEntry) {
        this.confirmationMessage("Are you sure?", "Do you want to delete log item with index '" + log.Index +"' from cluster log?", {
            buttons: ["Cancel", "I understand the risk, delete"]
        })
            .done(result => {
                if (result.can) {
                    new removeEntryFromLogCommand(log.Index)
                        .execute()
                    
                }
            });
    }

    customInlinePreview(log: LogEntry) {
        new getClusterLogEntryCommand(log.Index)
            .execute()
            .done((entry) => {
                app.showBootstrapDialog(new showDataDialog("Cluster Log Entry", JSON.stringify(entry, null, 4), "javascript"));
            });
    }
    
    private fetchClusterLog() {
        return new getClusterLogCommand()
            .execute()
            .done(log => {
                this.clusterLog(log);
            });
    }

    openInstallationDetails() {
        if (this.clusterLog().Role === "Follower") {
            const log = this.clusterLog() as Raven.Server.Rachis.FollowerDebugView;
            if (log.Phase === "Snapshot") {
                const items = log.RecentMessages;
                const dialog = new debugAdvancedClusterSnapshotInstallation(items);
                app.showBootstrapDialog(dialog);
            }
        }
    }

    openCriticalError() {
        const alertId = this.clusterLog().Log.CriticalError.Id;
        const criticalErrorAlert = notificationCenter.instance.globalNotifications().find(x => x.id === alertId);
        if (!criticalErrorAlert) {
            messagePublisher.reportError("Unable to find critical error alert");
        }
        
        notificationCenter.instance.openDetails(criticalErrorAlert);
    }
    
    refresh() {
        this.spinners.refresh(true);
        
        this.fetchClusterLog()
            .done(() => this.gridController().reset())
            .always(() => this.spinners.refresh(false));
    }

    showConnectionDetails(connection: Raven.Server.Rachis.RaftDebugView.PeerConnection) {
        app.showBootstrapDialog(new showDataDialog("Connection details", JSON.stringify(connection, null, 4), "javascript"));
    }
}

export = clusterDebug;
