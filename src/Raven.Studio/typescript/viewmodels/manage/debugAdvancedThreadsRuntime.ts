import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");

import app = require("durandal/app")
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import prismjs = require("prismjs");
import threadStackTrace = require("viewmodels/manage/threadStackTrace");
import threadsInfoWebSocketClient = require("common/threadsInfoWebSocketClient");
import eventsCollector = require("common/eventsCollector");

class debugAdvancedThreadsRuntime extends viewModelBase {

    view = require("views/manage/debugAdvancedThreadsRuntime.html");

    allData = ko.observable<Raven.Server.Dashboard.ThreadInfo[]>();
    filteredData = ko.observable<Raven.Server.Dashboard.ThreadInfo[]>();

    private liveClient = ko.observable<threadsInfoWebSocketClient>();
    private gridController = ko.observable<virtualGridController<Raven.Server.Dashboard.ThreadInfo>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Dashboard.ThreadInfo>();

    isConnectedToWebSocket: KnockoutComputed<boolean>;
    
    threadsCount: KnockoutComputed<number>;
    dedicatedThreadsCount = ko.observable<number>(0);
    machineCpuUsage = ko.observable<number>(0);
    serverCpuUsage = ko.observable<number>(0);

    isPause = ko.observable<boolean>(false);
    
    filter = ko.observable<string>();
    
    constructor() {
        super();
        
        this.isConnectedToWebSocket = ko.pureComputed(() => this.liveClient() && this.liveClient().isConnected());
        
        this.threadsCount = ko.pureComputed(() => {
            const data = this.filteredData();
            
            if (data) {
                return data.length;
            }
            return 0;
        });
        
        this.filter.throttle(500).subscribe(() => this.filterEntries());
    }
    
     private filterEntries() {
        if (this.gridController()) {
            const filter = this.filter();
            if (filter) {
                this.filteredData(this.allData().filter(item => this.matchesFilter(item)));
            } else {
                this.filteredData(this.allData().slice());
            }
    
            this.gridController().reset(true);
        } else {
            this.filteredData(this.allData().slice());
        }
    }
    
    private matchesFilter(item: Raven.Server.Dashboard.ThreadInfo): boolean {
        const filter = this.filter();
        if (!filter) {
            return true;
        }
        const filterLowered = filter.toLocaleLowerCase();
        
        return item.Name.toLocaleLowerCase().includes(filterLowered) ||
               item.Id.toString().includes(filterLowered);
    }
    
    compositionComplete(): void {
        super.compositionComplete();

        const fetcher = () => {
            const data = this.filteredData() || [];

            return $.when({
                totalResultCount: data.length,
                items: data
            } as pagedResult<Raven.Server.Dashboard.ThreadInfo>);
        };

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.setDefaultSortBy(2, "desc");
        grid.init(fetcher, () => {
                return [
                    new actionColumn<Raven.Server.Dashboard.ThreadInfo>(grid, (x) => this.showStackTrace(x), "Stack",
                                () => `<i title="Click to view Stack Trace" class="icon-thread-stack-trace"></i>`, "55px"),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.Name, "Name", "20%", {
                        sortable: "string"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => `${(x.CpuUsage === 0 ? "0" : generalUtils.formatNumberToStringFixed(x.CpuUsage, 2))}%`, "Current CPU %", "10%", {
                        sortable: x => x.CpuUsage,
                        defaultSortOrder: "desc"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.UnmanagedAllocationsInBytes ? generalUtils.formatBytesToSize(x.UnmanagedAllocationsInBytes, 2) : "N/A", "Unmanaged Allocations", "10%", {
                        sortable: x => x.UnmanagedAllocationsInBytes ?? 0,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.ReadIoOpsPerSecLast != null ? generalUtils.formatNumberToStringFixed(x.ReadIoOpsPerSecLast, 0) : "N/A", "Read IOPS", "8%", {
                        sortable: x => x.ReadIoOpsPerSecLast ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.WriteIoOpsPerSecLast != null ? generalUtils.formatNumberToStringFixed(x.WriteIoOpsPerSecLast, 0) : "N/A", "Write IOPS", "8%", {
                        sortable: x => x.WriteIoOpsPerSecLast ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.ReadThroughputKbPerSecLast != null ? generalUtils.formatNumberToStringFixed(x.ReadThroughputKbPerSecLast, 2) + " KB/s" : "N/A", "Read KB/s", "8%", {
                        sortable: x => x.ReadThroughputKbPerSecLast ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.WriteThroughputKbPerSecLast != null ? generalUtils.formatNumberToStringFixed(x.WriteThroughputKbPerSecLast, 2) + " KB/s" : "N/A", "Write KB/s", "8%", {
                        sortable: x => x.WriteThroughputKbPerSecLast ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.ReadIoOpsTotal != null ? generalUtils.formatNumberToStringFixed(x.ReadIoOpsTotal, 0) : "N/A", "Total Read IOPS", "8%", {
                        sortable: x => x.ReadIoOpsTotal ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.WriteIoOpsTotal != null ? generalUtils.formatNumberToStringFixed(x.WriteIoOpsTotal, 0) : "N/A", "Total Write IOPS", "8%", {
                        sortable: x => x.WriteIoOpsTotal ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.ReadThroughputKbTotal != null ? generalUtils.formatNumberToStringFixed(x.ReadThroughputKbTotal, 2) + " KB" : "N/A", "Total Read KB", "8%", {
                        sortable: x => x.ReadThroughputKbTotal ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.WriteThroughputKbTotal != null ? generalUtils.formatNumberToStringFixed(x.WriteThroughputKbTotal, 2) + " KB" : "N/A", "Total Write KB", "8%", {
                        sortable: x => x.WriteThroughputKbTotal ?? -1,
                        defaultSortOrder: "desc",
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => generalUtils.formatTimeSpan(x.Duration, false), "Overall CPU Time", "10%", {
                        sortable: x => x.Duration,
                        defaultSortOrder: "desc"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.Id + " (" + (x.ManagedThreadId || "n/a") + ")", "Thread Id", "10%", {
                        sortable: x => x.Id
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => generalUtils.formatUtcDateAsLocal(x.StartingTime), "Start Time", "10%", {
                        sortable: x => x.StartingTime
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.State, "State", "10%", {
                        sortable: "string"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.WaitReason, "Wait reason", "10%")
                ];
            }
        );

        this.columnPreview.install("virtual-grid", ".js-threads-runtime-tooltip",
            (entry: Raven.Server.Dashboard.ThreadInfo,
                column: textColumn<Raven.Server.Dashboard.ThreadInfo>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                if (column.header === "Overall CPU Time") {
                    const timings = {
                        StartTime: entry.StartingTime,
                            TotalProcessorTime: entry.TotalProcessorTime,
                        PrivilegedProcessorTime: entry.PrivilegedProcessorTime,
                        UserProcessorTime: entry.UserProcessorTime
                    };
                    const json = JSON.stringify(timings, null, 4);
                    const html = prismjs.highlight(json, prismjs.languages.javascript, "js");
                    onValue(html, json);
                } else if (column.header === "Start Time") {
                    onValue(moment.utc(entry.StartingTime), entry.StartingTime);
                } else {
                    const value = column.getCellValue(entry);
                    onValue(generalUtils.escapeHtml(value), value);
                }
            });

        this.connectWebSocket();
    }

    connectWebSocket() {
        eventsCollector.default.reportEvent("threads-info", "connect");

        const ws = new threadsInfoWebSocketClient(data => this.onData(data));
        this.liveClient(ws);
    }
    
    private onData(data: Raven.Server.Dashboard.ThreadsInfo) {
        this.allData(data.List);
        this.machineCpuUsage(data.CpuUsage);
        this.serverCpuUsage(data.ProcessCpuUsage);
        this.dedicatedThreadsCount(data.DedicatedThreadsCount);

        this.filterEntries();
        
        this.gridController().reset(false);
    }

    deactivate() {
        super.deactivate();

        if (this.liveClient()) {
            this.liveClient().dispose();
    }
    }

    private showStackTrace(thread: Raven.Server.Dashboard.ThreadInfo) {
        app.showBootstrapDialog(new threadStackTrace(thread.Id, thread.Name));
    }

    pause() {
        eventsCollector.default.reportEvent("threads-info", "pause");

        if (this.liveClient()) {
            this.isPause(true);
            this.liveClient().dispose();
            this.liveClient(null);
}
    }

    resume() {
        this.connectWebSocket();
        this.isPause(false);
    }
}

export = debugAdvancedThreadsRuntime;
