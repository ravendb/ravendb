import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import getDebugThreadsRunawayCommand = require("commands/database/debug/getDebugThreadsRunawayCommand");

class debugAdvancedThreadsRuntime extends viewModelBase {

    allData = ko.observable<Raven.Server.Dashboard.ThreadInfo[]>();
    filteredData = ko.observable<Raven.Server.Dashboard.ThreadInfo[]>();

    private gridController = ko.observable<virtualGridController<Raven.Server.Dashboard.ThreadInfo>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Dashboard.ThreadInfo>();

    threadsCount: KnockoutComputed<number>;
    dedicatedThreadsCount: KnockoutComputed<number>;
    filter = ko.observable<string>();
    
    spinners = {
        refresh: ko.observable<boolean>(false),
    };

    constructor() {
        super();
        
        this.threadsCount = ko.pureComputed(() => {
            const data = this.filteredData();
            
            if (data) {
                return data.length;
            }
            return 0;
        });
        
        this.dedicatedThreadsCount = ko.pureComputed(() => {
            const data = this.filteredData();
            
            if (data) {
                return data.filter(x => x.Name !== "Unknown" && x.Name !== "Unmanaged Thread").length;
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
    
    private matchesFilter(item: Raven.Server.Dashboard.ThreadInfo) {
        const filter = this.filter();
        if (!filter) {
            return true;
        }
        const filterLowered = filter.toLocaleLowerCase();
        return item.Name.toLocaleLowerCase().includes(filterLowered);
    }
    
    activate(args: any) {
        super.activate(args);

        return this.loadThreadsRunaway();
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
        grid.init(fetcher, () => {
                return [
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.Name, "Name", "25%", {
                        sortable: "string"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => `${(x.CpuUsage === 0 ? "0" : generalUtils.formatNumberToStringFixed(x.CpuUsage, 2))}%`, "Current CPU %", "10%", {
                        sortable: x => x.CpuUsage,
                        defaultSortOrder: "desc"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => generalUtils.formatTimeSpan(x.Duration, false), "Overall CPU Time", "10%", {
                        sortable: x => x.Duration,
                        defaultSortOrder: "desc"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.Id + " (" + (x.ManagedThreadId || "n/a") + ")", "Thread Id", "10%", {
                        sortable: x => x.Id
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => generalUtils.formatUtcDateAsLocal(x.StartingTime), "Start Time", "20%", {
                        sortable: x => x.StartingTime
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.State, "State", "10%", {
                        sortable: "string"
                    }),
                    new textColumn<Raven.Server.Dashboard.ThreadInfo>(grid, x => x.WaitReason, "Wait reason", "15%")
                ];
            }
        );

        this.columnPreview.install("virtual-grid", ".js-threads-runtime-tooltip",
            (entry: Raven.Server.Dashboard.ThreadInfo,
                column: textColumn<Raven.Server.Dashboard.ThreadInfo>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                if (column.header === "Overall CPU Time") {
                    const timings = {
                        StartTime: entry.StartingTime,
                            TotalProcessorTime: entry.TotalProcessorTime,
                        PrivilegedProcessorTime: entry.PrivilegedProcessorTime,
                        UserProcessorTime: entry.UserProcessorTime
                    };
                    const json = JSON.stringify(timings, null, 4);
                    const html = Prism.highlight(json, (Prism.languages as any).javascript);
                    onValue(html, json);
                } else if (column.header === "Start Time") {
                    onValue(moment.utc(entry.StartingTime), entry.StartingTime);
                } else {
                    const value = column.getCellValue(entry);
                    onValue(value);
                }
            });
    }

    private loadThreadsRunaway() {
        return new getDebugThreadsRunawayCommand()
            .execute()
            .done(response => {
                this.allData(response);
                this.filterEntries();
            });
    }

    refresh() {
        this.spinners.refresh(true);
        return this.loadThreadsRunaway()
            .done(() => this.gridController().reset(true))
            .always(() => this.spinners.refresh(false));
    }

}

export = debugAdvancedThreadsRuntime;
