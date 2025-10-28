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
import awesomeMultiselect = require("common/awesomeMultiselect");

type Unit = "" | "%" | "B" | "KB" | "KB/s";
type ThreadInfo = Raven.Server.Dashboard.ThreadInfo;

class debugAdvancedThreadsRuntime extends viewModelBase {
    view = require("views/manage/debugAdvancedThreadsRuntime.html");

    allData = ko.observable<ThreadInfo[]>();
    filteredData = ko.observable<ThreadInfo[]>();

    private liveClient = ko.observable<threadsInfoWebSocketClient>();
    private gridController = ko.observable<virtualGridController<ThreadInfo>>();
    private columnPreview = new columnPreviewPlugin<ThreadInfo>();

    isConnectedToWebSocket: KnockoutComputed<boolean>;
    
    threadsCount: KnockoutComputed<number>;
    dedicatedThreadsCount = ko.observable<number>(0);
    machineCpuUsage = ko.observable<number>(0);
    serverCpuUsage = ko.observable<number>(0);

    isPause = ko.observable<boolean>(false);
    
    filter = ko.observable<string>();

    allColumnHeaders = [
        "Stack",
        "Name",
        "CPU%",
        "Unmanaged Alloc.",
        "IOPS",
        "IOPS Read",
        "IOPS Write",
        "IO Throughput",
        "IO Throughput Read",
        "IO Throughput Write",
        "Total IOPS",
        "Total IOPS Read",
        "Total IOPS Write",
        "Total IO Throughput",
        "Total IO Throughput Read",
        "Total IO Throughput Write",
        "Total CPU Time",
        "Thread ID",
        "Start Time",
        "State",
        "Wait reason",
    ] as const;

    visibleColumnHeaders = ko.observableArray<(typeof this.allColumnHeaders)[number]>([
        "Stack",
        "Name",
        "CPU%",
        "Unmanaged Alloc.",
        "IOPS",
        "IO Throughput",
        "Total IOPS",
        "Total IO Throughput",
        "Total CPU Time",
        "Thread ID",
        "Start Time",
        "State",
        "Wait reason",
    ]);
    
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
        
        this.filter.throttle(300).subscribe(() => this.filterEntries());
        this.visibleColumnHeaders.subscribe(() => this.filterEntries(true));
    }
    
    attached() {
        super.attached();
        
        awesomeMultiselect.build($("#visibleColumnsSelector"), opts => {
            opts.includeSelectAllOption = true;
            opts.nSelectedText = " columns selected";
            opts.allSelectedText = "All columns selected";
        });
    }
    
     private filterEntries(hard: boolean = false) {
        if (this.gridController()) {
            const filter = this.filter();
            if (filter) {
                this.filteredData(this.allData().filter(item => this.matchesFilter(item)));
            } else {
                this.filteredData(this.allData().slice());
            }
    
            this.gridController().reset(hard);
        } else {
            this.filteredData(this.allData().slice());
        }
    }
    
    private matchesFilter(item: ThreadInfo): boolean {
        const filter = this.filter();
        if (!filter) {
            return true;
        }
        const filterLowered = filter.toLocaleLowerCase();
        
        return item.Name.toLocaleLowerCase().includes(filterLowered) ||
               item.Id.toString().includes(filterLowered);
    }
    
    private getStringValue(value: number, defaultFractionDigits: number, originalUnit: Unit = ""): string {
        if (value == null) {
            return "N/A";
        }

        const fractionDigits = value === 0 ? 0 : defaultFractionDigits;

        switch (originalUnit) {
            case "":
                return generalUtils.formatNumberToStringFixed(value, fractionDigits);
            case "%":
                return generalUtils.formatNumberToStringFixed(value, fractionDigits) + "%";
            case "B":
                return generalUtils.formatBytesToSize(value, fractionDigits);
            case "KB":
                return generalUtils.formatBytesToSize(value * 1024, fractionDigits);
            case "KB/s":
                return generalUtils.formatBytesToSize(value * 1024, fractionDigits) + "/s";
            default:
                generalUtils.assertUnreachable(originalUnit);
        }
    }

    private getSortableValue(value: number | string): number | string {
        if (value == null) {
            return -1;
        }

        return value;
    }

    compositionComplete(): void {
        super.compositionComplete();

        const fetcher = () => {
            const data = this.filteredData() || [];

            return $.when({
                totalResultCount: data.length,
                items: data
            } as pagedResult<ThreadInfo>);
        };
        
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.setDefaultSortBy(2, "desc");
        grid.init(fetcher, () => {
                type ColumnHeader = (typeof this.allColumnHeaders)[number];

                const columns = [
                    new actionColumn<ThreadInfo>(grid, (x) => this.showStackTrace(x), "Stack" satisfies ColumnHeader, () => `<i title="Click to view Stack Trace" class="icon-thread-stack-trace"></i>`, "55px"),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => x.Name, "Name", "20%", {
                        sortable: "string"
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.CpuUsage, 2, "%"), "CPU%", "5%", {
                        sortable: x => this.getSortableValue(x.CpuUsage),
                        defaultSortOrder: "desc",
                        headerTitle: "Current CPU usage percentage",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.UnmanagedAllocationsInBytes, 2, "B"), "Unmanaged Alloc.", "7%", {
                        sortable: x => this.getSortableValue(x.UnmanagedAllocationsInBytes),
                        headerTitle: "Unmanaged allocations in bytes",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.IoOpsPerSecLast, 0), "IOPS", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.IoOpsPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ReadIoOpsPerSecLast, 0), "IOPS Read", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ReadIoOpsPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.WriteIoOpsPerSecLast, 0), "IOPS Write", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.WriteIoOpsPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ThroughputKbPerSecLast, 2, "KB/s"), "IO Throughput", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ThroughputKbPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ReadThroughputKbPerSecLast, 2, "KB/s"), "IO Throughput Read", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ReadThroughputKbPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.WriteThroughputKbPerSecLast, 2, "KB/s"), "IO Throughput Write", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.WriteThroughputKbPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.IoOpsTotal, 0), "Total IOPS", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.IoOpsTotal),
                        headerTitle: "Total IOPS aggregated since this view was open",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ReadIoOpsTotal, 0), "Total IOPS Read", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ReadIoOpsTotal),
                        headerTitle: "Total IOPS Read aggregated since this view was open",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.WriteIoOpsTotal, 0), "Total IOPS Write", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.WriteIoOpsTotal),
                        headerTitle: "Total IOPS Write aggregated since this view was open",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ThroughputKbTotal, 2, "KB"), "Total IO Throughput", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ThroughputKbTotal),
                        headerTitle: "Total IO Throughput aggregated since this view was open",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ReadThroughputKbTotal, 2, "KB"), "Total IO Throughput Read", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ReadThroughputKbTotal),
                        headerTitle: "Total IO Throughput Read aggregated since this view was open",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.WriteThroughputKbTotal, 2, "KB"), "Total IO Throughput Write", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.WriteThroughputKbTotal),
                        headerTitle: "Total IO Throughput Write aggregated since this view was open",
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => generalUtils.formatTimeSpan(x.Duration, false), "Total CPU Time", "7%", {
                        sortable: x => this.getSortableValue(x.Duration),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => x.Id + " (" + (x.ManagedThreadId || "N/A") + ")", "Thread ID", "7%", {
                        sortable: x => this.getSortableValue(x.Id)
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => generalUtils.formatUtcDateAsLocal(x.StartingTime), "Start Time", "9%", {
                        sortable: x => this.getSortableValue(x.StartingTime)
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => x.State, "State", "5%", {
                        sortable: "string"
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => x.WaitReason, "Wait reason", "7%")
                ];

                return columns.filter(column => this.visibleColumnHeaders().includes(column.header as ColumnHeader));
            }
        );

        this.columnPreview.install("virtual-grid", ".js-threads-runtime-tooltip",
            (entry: ThreadInfo, column: textColumn<ThreadInfo>, _: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
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
    }

    deactivate() {
        super.deactivate();

        if (this.liveClient()) {
            this.liveClient().dispose();
    }
    }

    private showStackTrace(thread: ThreadInfo) {
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
