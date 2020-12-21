import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import fileDownloader = require("common/fileDownloader");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import eventsCollector = require("common/eventsCollector");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import trafficWatchWebSocketClient = require("common/trafficWatchWebSocketClient");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import generalUtils = require("common/generalUtils");
import awesomeMultiselect = require("common/awesomeMultiselect");

class typeData {
    count = ko.observable<number>(0);
    propertyName: Raven.Client.Documents.Changes.TrafficWatchChangeType;

    constructor(propertyName: Raven.Client.Documents.Changes.TrafficWatchChangeType) {
        this.propertyName = propertyName;
    }
    
    inc() {
        this.count(this.count() + 1);
    }
}

class trafficWatch extends viewModelBase {
    
    static maxBufferSize = 200000;
    
    private liveClient = ko.observable<trafficWatchWebSocketClient>();
    private allData = [] as Raven.Client.Documents.Changes.TrafficWatchChange[];
    private filteredData = [] as Raven.Client.Documents.Changes.TrafficWatchChange[];

    private gridController = ko.observable<virtualGridController<Raven.Client.Documents.Changes.TrafficWatchChange>>();
    private columnPreview = new columnPreviewPlugin<Raven.Client.Documents.Changes.TrafficWatchChange>();

    private readonly allTypeData: Raven.Client.Documents.Changes.TrafficWatchChangeType[] =
        ["BulkDocs", "Counters", "Documents", "Hilo", "Index", "MultiGet", "None", "Operations", "Queries", "Streams", "Subscriptions"];
    private filteredTypeData = this.allTypeData.map(x => new typeData(x));
    private selectedTypeNames = ko.observableArray<string>(this.allTypeData.splice(0));
    onlyErrors = ko.observable<boolean>(false);

    stats = {
        count: ko.observable<string>(),
        min: ko.observable<string>(),
        avg: ko.observable<string>(),
        max: ko.observable<string>(),
        percentile_90: ko.observable<string>(),
        percentile_99: ko.observable<string>(),
        percentile_99_9: ko.observable<string>()
    };
    
    filter = ko.observable<string>();
    
    private appendElementsTask: number;

    isBufferFull = ko.observable<boolean>();
    tailEnabled = ko.observable<boolean>(true);
    private duringManualScrollEvent = false;

    private typesMultiSelectRefreshThrottle = _.throttle(() => this.syncMultiSelect(), 1000);

    isPauseLogs = ko.observable<boolean>(false);
    
    constructor() {
        super();

        this.updateStats();

        this.filter.throttle(500).subscribe(() => this.refresh());
        this.onlyErrors.subscribe(() => this.refresh());
        this.selectedTypeNames.subscribe(() => this.refresh());
    }
    
    activate(args: any) {
        super.activate(args);
        
        if (args && args.filter) {
            this.filter(args.filter);
        }
        this.updateHelpLink('EVEP6I');
    }

    deactivate() {
        super.deactivate();

        if (this.liveClient()) {
            this.liveClient().dispose();
        }
    }
    
    attached() {
        super.attached();
        awesomeMultiselect.build($("#visibleTypesSelector"), opts => {
            opts.enableHTML = true;
            opts.includeSelectAllOption = true;
            opts.nSelectedText = " Types Selected";
            opts.allSelectedText = "All Types Selected";
            opts.optionLabel = (element: HTMLOptionElement) => {
                const propertyName = $(element).text();
                const typeItem = this.filteredTypeData.find(x => x.propertyName === propertyName);
                return `<span class="name">${generalUtils.escape(propertyName)}</span><span class="badge">${typeItem.count().toLocaleString()}</span>`;
            };
        });
    }

    private syncMultiSelect() {
        awesomeMultiselect.rebuild($("#visibleTypesSelector"));
    }

    private refresh() {
        this.gridController().reset(false);
    }

    private matchesFilters(item: Raven.Client.Documents.Changes.TrafficWatchChange) {
        const textFilterLower = this.filter() ? this.filter().trim().toLowerCase() : "";
        const uri = item.RequestUri.toLocaleLowerCase();
        const customInfo = item.CustomInfo;
        
        const textFilterMatch = textFilterLower ? uri.includes(textFilterLower) || (customInfo && customInfo.toLocaleLowerCase().includes(textFilterLower)) : true;
        const typeMatch = _.includes(this.selectedTypeNames(), item.Type);
        const statusMatch = !this.onlyErrors() || item.ResponseStatusCode >= 400;
        
        return textFilterMatch && typeMatch && statusMatch;
    }

    private updateStats() {
        if (!this.filteredData.length) {
           this.statsNotAvailable();
        } else {
            let sum = 0;
            let min = this.filteredData[0].ElapsedMilliseconds;
            let max = this.filteredData[0].ElapsedMilliseconds;
            
            for (let i = 0; i < this.filteredData.length; i++) {
                const item = this.filteredData[i];
                
                if (item.ResponseStatusCode === 101) {
                    // it is websocket - don't include in stats
                    continue;
                }

                if (item.ElapsedMilliseconds < min) {
                    min = item.ElapsedMilliseconds;
                }

                if (item.ElapsedMilliseconds > max) {
                    max = item.ElapsedMilliseconds;
                }

                sum += item.ElapsedMilliseconds;
            }

            this.stats.min(generalUtils.formatTimeSpan(min, false));
            this.stats.max(generalUtils.formatTimeSpan(max, false));
            this.stats.count(this.filteredData.length.toLocaleString());
            if (this.filteredData.length) {
                this.stats.avg(generalUtils.formatTimeSpan(sum / this.filteredData.length));
                this.updatePercentiles();
            } else {
                this.statsNotAvailable();
            }
        }
    }
    
    private statsNotAvailable() {
        this.stats.avg("n/a");
        this.stats.min("n/a");
        this.stats.max("n/a");
        this.stats.count("0");

        this.stats.percentile_90("n/a");
        this.stats.percentile_99("n/a");
        this.stats.percentile_99_9("n/a");
    }
    
    private updatePercentiles() {
        const timings = [] as number[];

        for (let i = this.filteredData.length - 1; i >= 0; i--) {
            const item = this.filteredData[i];

            if (item.ResponseStatusCode === 101) {
                // it is websocket - don't include in stats
                continue;
            }

            if (timings.length === 2048) {
                // compute using max 2048 latest values
                break;
            }

            timings.push(item.ElapsedMilliseconds);
        }

        timings.sort((a, b) => a - b);
        
        this.stats.percentile_90(generalUtils.formatTimeSpan(timings[Math.ceil(90 / 100 * timings.length) - 1]));
        this.stats.percentile_99(generalUtils.formatTimeSpan(timings[Math.ceil(99 / 100 * timings.length) - 1]));
        this.stats.percentile_99_9(generalUtils.formatTimeSpan(timings[Math.ceil(99.9 / 100 * timings.length) - 1]));
    }

    compositionComplete() {
        super.compositionComplete();

        $('.traffic-watch [data-toggle="tooltip"]').tooltip();

        const rowHighlightRules = (item: Raven.Client.Documents.Changes.TrafficWatchChange) => {
            const responseCode = item.ResponseStatusCode.toString();
            if (responseCode.startsWith("4")) {
                return "bg-warning";
            } else if (responseCode.startsWith("5")) {
                return "bg-danger";
            }
            return "";
        };
        
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init((s, t) => this.fetchTraffic(s, t), () =>
            [
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => generalUtils.formatUtcDateAsLocal(x.TimeStamp), "Timestamp", "20%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => x.ResponseStatusCode, "Status", "8%", {
                    extraClass: rowHighlightRules,
                    sortable: "number"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => x.DatabaseName, "Database Name", "8%", {
                    extraClass: rowHighlightRules,
                    sortable: "string",
                    customComparator: generalUtils.sortAlphaNumeric
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => x.ElapsedMilliseconds, "Duration", "8%", {
                    extraClass: rowHighlightRules,
                    sortable: "number"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => x.HttpMethod, "Method", "6%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => x.Type, "Type", "6%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => x.CustomInfo, "Custom Info", "8%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>(grid, x => x.RequestUri, "URI", "35%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                })
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-traffic-watch-tooltip", 
            (item: Raven.Client.Documents.Changes.TrafficWatchChange, column: textColumn<Raven.Client.Documents.Changes.TrafficWatchChange>, 
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
            if (column.header === "URI") {
                onValue(item.RequestUri);
            } else if (column.header === "Timestamp") {
                onValue(moment.utc(item.TimeStamp), item.TimeStamp); 
            } else if (column.header === "Custom Info") {
                onValue(generalUtils.escapeHtml(item.CustomInfo), item.CustomInfo);
            }
        });

        $(".traffic-watch .viewport").on("scroll", () => {
            if (!this.duringManualScrollEvent && this.tailEnabled()) {
                this.tailEnabled(false);
            }

            this.duringManualScrollEvent = false;
        });
        this.connectWebSocket();
    }

    private fetchTraffic(skip: number, take: number): JQueryPromise<pagedResult<Raven.Client.Documents.Changes.TrafficWatchChange>> {
        const textFilterDefined = this.filter();
        const filterUsingType = this.selectedTypeNames().length !== this.filteredTypeData.length;
        const filterUsingStatus = this.onlyErrors();
        
        if (textFilterDefined || filterUsingType || filterUsingStatus) {
            this.filteredData = this.allData.filter(item => this.matchesFilters(item));
        } else {
            this.filteredData = this.allData;
        }
        this.updateStats();

        return $.when({
            items: this.filteredData,
            totalResultCount: this.filteredData.length
        });
    }

    connectWebSocket() {
        eventsCollector.default.reportEvent("traffic-watch", "connect");
        
        const ws = new trafficWatchWebSocketClient(data => this.onData(data));
        this.liveClient(ws);
    }
    
    isConnectedToWebSocket() {
        if (this.liveClient() && this.liveClient().connectionOpened()) {
            return true;
        }        
        return false;
    }

    private onData(data: Raven.Client.Documents.Changes.TrafficWatchChange) {
        if (this.allData.length === trafficWatch.maxBufferSize) {
            this.isBufferFull(true);
            this.pause();
            return;
        }
        
        this.allData.push(data);
        
        this.filteredTypeData.find(x => x.propertyName === data.Type).inc();
        this.typesMultiSelectRefreshThrottle();

        if (!this.appendElementsTask) {
            this.appendElementsTask = setTimeout(() => this.onAppendPendingEntries(), 333);
        }
    }

    clearTypeCounter(): void {
        this.filteredTypeData.forEach(x => x.count(0));
        this.syncMultiSelect();
    }

    private onAppendPendingEntries() {
        this.appendElementsTask = null;
        
        this.gridController().reset(false);
        
        if (this.tailEnabled()) {
            this.scrollDown();
        }
    }
    
    pause() {
        eventsCollector.default.reportEvent("traffic-watch", "pause");
        
        if (this.liveClient()) {
            this.isPauseLogs(true);
            this.liveClient().dispose();
            this.liveClient(null);
        }
    }
    
    resume() {
        this.connectWebSocket();
        this.isPauseLogs(false);
    }

    clear() {
        eventsCollector.default.reportEvent("traffic-watch", "clear");
        this.allData = [];
        this.filteredData = [];
        this.isBufferFull(false);
        this.clearTypeCounter();
        this.gridController().reset(true);

        this.updateStats();

        // set flag to true, since grid reset is async
        this.duringManualScrollEvent = true;
        this.tailEnabled(true);
        if (!this.liveClient()) {
            this.resume();
        }
    }

    exportToFile() {
        eventsCollector.default.reportEvent("traffic-watch", "export");

        const now = moment().format("YYYY-MM-DD HH-mm");
        fileDownloader.downloadAsJson(this.allData, "traffic-watch-" + now + ".json");
    }

    toggleTail() {
        this.tailEnabled.toggle();

        if (this.tailEnabled()) {
            this.scrollDown();
        }
    }

    private scrollDown() {
        this.duringManualScrollEvent = true;

        this.gridController().scrollDown();
    }
}

export = trafficWatch;
