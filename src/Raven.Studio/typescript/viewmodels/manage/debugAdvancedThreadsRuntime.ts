import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import getDebugThreadsRunawayCommand = require("commands/database/debug/getDebugThreadsRunawayCommand");

class debugAdvancedThreadsRuntime extends viewModelBase {

    allData = ko.observable<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo[]>();
    filteredData = ko.observable<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo[]>();

    private gridController = ko.observable<virtualGridController<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>();

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
    
    private matchesFilter(item: Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo) {
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
            } as pagedResult<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>);
        };

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(fetcher, () =>
            [
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.Name, "Name", "25%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => (x.ManagedThreadId || 'n/a') + " (" + x.Id + ")", "Thread Id", "10%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => generalUtils.formatUtcDateAsLocal(x.StartingTime), "Start Time", "20%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => generalUtils.formatTimeSpan(x.Duration, false), "Duration", "10%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.State, "State", "10%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.WaitReason, "Wait reason", "20%"),
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-threads-runtime-tooltip",
            (entry: Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo,
             column: textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                if (column.header === "Duration") {
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
