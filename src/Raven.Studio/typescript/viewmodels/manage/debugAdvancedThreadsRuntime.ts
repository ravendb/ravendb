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

interface IoStatsWithTotals extends Raven.Server.Dashboard.IoStats {
    IoSyscallsTotal?: number;
    ReadIoSyscallsTotal?: number;
    WriteIoSyscallsTotal?: number;
    ThroughputKbTotal?: number;
    ReadThroughputKbTotal?: number;
    WriteThroughputKbTotal?: number;
}

interface ThreadInfo extends Omit<Raven.Server.Dashboard.ThreadInfo, "IoStats"> {
    IoStats?: IoStatsWithTotals;
}

interface IoSnapshot {
    syscr: number;
    syscw: number;
    readBytes: number;
    writeBytes: number;
}

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
    
    showTotalsSinceThreadCreation = ko.observable<boolean>(false);
    
    private initialSnapshots = new Map<string, IoSnapshot>();

    allColumnHeaders = [
        "Stack",
        "Name",
        "CPU%",
        "Unmanaged Alloc.",
        "IO SysCalls",
        "IO SysCalls Read",
        "IO SysCalls Write",
        "IO Throughput",
        "IO Throughput Read",
        "IO Throughput Write",
        "Total IO SysCalls",
        "Total IO SysCalls Read",
        "Total IO SysCalls Write",
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
        "IO SysCalls",
        "IO Throughput",
        "Total IO SysCalls",
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
        this.showTotalsSinceThreadCreation.subscribe(() => this.filterEntries(true));
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

                const totalsSuffix = this.showTotalsSinceThreadCreation()
                    ? "aggregated since thread creation"
                    : "aggregated since this view was open";

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
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.IoSyscallsPerSecLast, 0), "IO SysCalls", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.IoSyscallsPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ReadIoSyscallsPerSecLast, 0), "IO SysCalls Read", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ReadIoSyscallsPerSecLast),
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.WriteIoSyscallsPerSecLast, 0), "IO SysCalls Write", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.WriteIoSyscallsPerSecLast),
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
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.IoSyscallsTotal, 0), "Total IO SysCalls", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.IoSyscallsTotal),
                        headerTitle: `Total IO SysCalls ${totalsSuffix}`,
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ReadIoSyscallsTotal, 0), "Total IO SysCalls Read", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ReadIoSyscallsTotal),
                        headerTitle: `Total IO SysCalls Read ${totalsSuffix}`,
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.WriteIoSyscallsTotal, 0), "Total IO SysCalls Write", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.WriteIoSyscallsTotal),
                        headerTitle: `Total IO SysCalls Write ${totalsSuffix}`,
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ThroughputKbTotal, 2, "KB"), "Total IO Throughput", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ThroughputKbTotal),
                        headerTitle: `Total IO Throughput ${totalsSuffix}`,
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.ReadThroughputKbTotal, 2, "KB"), "Total IO Throughput Read", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.ReadThroughputKbTotal),
                        headerTitle: `Total IO Throughput Read ${totalsSuffix}`,
                    }),
                    new textColumn<ThreadInfo, ColumnHeader>(grid, x => this.getStringValue(x.IoStats?.WriteThroughputKbTotal, 2, "KB"), "Total IO Throughput Write", "7%", {
                        sortable: x => this.getSortableValue(x.IoStats?.WriteThroughputKbTotal),
                        headerTitle: `Total IO Throughput Write ${totalsSuffix}`,
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

        this.initialSnapshots.clear();

        const ws = new threadsInfoWebSocketClient(data => this.onData(data));
        this.liveClient(ws);
    }
    
    private onData(data: Raven.Server.Dashboard.ThreadsInfo) {
        const KB = 1024;
        const threads = data.List as ThreadInfo[];
        const sinceCreation = this.showTotalsSinceThreadCreation();

        for (const thread of threads) {
            if (thread.IoStats && thread.IoStats.Syscr != null) {
                const snapshotKey = thread.Id + "_" + thread.StartingTime;
                const current: IoSnapshot = {
                    syscr: thread.IoStats.Syscr,
                    syscw: thread.IoStats.Syscw,
                    readBytes: thread.IoStats.ReadBytes,
                    writeBytes: thread.IoStats.WriteBytes
                };

                if (!this.initialSnapshots.has(snapshotKey)) {
                    this.initialSnapshots.set(snapshotKey, { ...current });
                }

                if (sinceCreation) {
                    // Use raw cumulative values (total since thread creation)
                    thread.IoStats.IoSyscallsTotal = current.syscr + current.syscw;
                    thread.IoStats.ThroughputKbTotal = (current.readBytes + current.writeBytes) / KB;
                    thread.IoStats.ReadIoSyscallsTotal = current.syscr;
                    thread.IoStats.WriteIoSyscallsTotal = current.syscw;
                    thread.IoStats.ReadThroughputKbTotal = current.readBytes / KB;
                    thread.IoStats.WriteThroughputKbTotal = current.writeBytes / KB;
                } else {
                    // Use snapshot deltas (total since monitoring started)
                    const initial = this.initialSnapshots.get(snapshotKey);

                    thread.IoStats.IoSyscallsTotal = (current.syscr - initial.syscr) + (current.syscw - initial.syscw);
                    thread.IoStats.ThroughputKbTotal = ((current.readBytes - initial.readBytes) + (current.writeBytes - initial.writeBytes)) / KB;
                    thread.IoStats.ReadIoSyscallsTotal = current.syscr - initial.syscr;
                    thread.IoStats.WriteIoSyscallsTotal = current.syscw - initial.syscw;
                    thread.IoStats.ReadThroughputKbTotal = (current.readBytes - initial.readBytes) / KB;
                    thread.IoStats.WriteThroughputKbTotal = (current.writeBytes - initial.writeBytes) / KB;
                }
            }
        }

        this.allData(threads);
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
