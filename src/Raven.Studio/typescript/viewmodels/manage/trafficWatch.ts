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
import getCertificatesCommand = require("commands/auth/getCertificatesCommand");

type trafficChangeType = Raven.Client.Documents.Changes.TrafficWatchChangeType | Raven.Client.ServerWide.Tcp.TcpConnectionHeaderMessage.OperationTypes; 

class typeData {
    count = ko.observable<number>(0);
    propertyName: trafficChangeType;

    constructor(propertyName: trafficChangeType) {
        this.propertyName = propertyName;
    }
    
    inc() {
        this.count(this.count() + 1);
    }
}

type certificateInfo = {
    name: string;
    clearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
}

class trafficWatch extends viewModelBase {
    
    static readonly usingHttps = location.protocol === "https:";
    
    static maxBufferSize = 200000;
    
    static dateTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    private liveClient = ko.observable<trafficWatchWebSocketClient>();
    private allData = [] as Raven.Client.Documents.Changes.TrafficWatchChangeBase[];
    
    private filteredData = [] as Raven.Client.Documents.Changes.TrafficWatchChangeBase[];
    private filteredDataHttp = [] as Raven.Client.Documents.Changes.TrafficWatchHttpChange[];
    private filteredDataTcp = [] as Raven.Client.Documents.Changes.TrafficWatchTcpChange[];
    
    private clientIps: string[] = [];

    certificatesCache = new Map<string, certificateInfo>();

    private gridController = ko.observable<virtualGridController<Raven.Client.Documents.Changes.TrafficWatchChangeBase>>();
    private columnPreview = new columnPreviewPlugin<Raven.Client.Documents.Changes.TrafficWatchChangeBase>();

    private readonly allTypeDataHttp: Raven.Client.Documents.Changes.TrafficWatchChangeType[] =
        ["BulkDocs", "Counters", "Documents", "Hilo", "Index", "MultiGet", "None", "Operations", "Queries", "Streams", "Subscriptions", "TimeSeries"];
    private readonly allTypeDataTcp: Raven.Client.ServerWide.Tcp.TcpConnectionHeaderMessage.OperationTypes[] =
        ["Cluster", "Drop", "Heartbeats", "None", "Ping", "Replication", "Subscription", "TestConnection"];
    
    private filteredTypeDataHttp = this.allTypeDataHttp.map(x => new typeData(x));
    private selectedTypeNamesHttp = ko.observableArray<string>(this.allTypeDataHttp.splice(0));
    
    private filteredTypeDataTcp = this.allTypeDataTcp.map(x => new typeData(x));
    private selectedTypeNamesTcp = ko.observableArray<string>(this.allTypeDataTcp.splice(0));
    
    onlyErrors = ko.observable<boolean>(false);

    stats = {
        clientCount: ko.observable<string>(),
        httpRequestCount: ko.observable<string>(),
        tcpOperationCount: ko.observable<string>(),
        min: ko.observable<string>(),
        avg: ko.observable<string>(),
        max: ko.observable<string>(),
        percentile_90: ko.observable<string>(),
        percentile_99: ko.observable<string>(),
        percentile_99_9: ko.observable<string>()
    };
    
    filter = ko.observable<string>();
    isDataFiltered = ko.observable<boolean>(false);
    
    private appendElementsTask: number;

    isBufferFull = ko.observable<boolean>();
    tailEnabled = ko.observable<boolean>(true);
    private duringManualScrollEvent = false;

    private typesMultiSelectRefreshThrottle = _.throttle(() => trafficWatch.syncMultiSelect(), 1000);

    isPauseLogs = ko.observable<boolean>(false);
    
    constructor() {
        super();

        this.updateStats();

        this.filter.throttle(500).subscribe(() => this.refresh());
        this.onlyErrors.subscribe(() => this.refresh());
        
        this.selectedTypeNamesHttp.subscribe(() => this.refresh());
        this.selectedTypeNamesTcp.subscribe(() => this.refresh());
    }
    
    activate(args: any) {
        super.activate(args);
        
        if (args && args.filter) {
            this.filter(args.filter);
        }
        this.updateHelpLink('EVEP6I');

        if (trafficWatch.usingHttps) {
            return this.loadCertificates();
        }
    }

    private loadCertificates() {
        return new getCertificatesCommand()
            .execute()
            .done(certificatesInfo => {
                if (certificatesInfo.Certificates) {
                    certificatesInfo.Certificates.forEach(cert => {
                        this.certificatesCache.set(cert.Thumbprint, {
                            name: cert.Name,
                            clearance: cert.SecurityClearance
                        });
                    })
                }
                
                if (certificatesInfo.WellKnownAdminCerts) {
                    certificatesInfo.WellKnownAdminCerts.forEach(wellKnownCert => {
                        this.certificatesCache.set(wellKnownCert, {
                            name: "Well known admin certificate",
                            clearance: "ClusterAdmin"
                        });
                    });
                }
            });
    }

    deactivate() {
        super.deactivate();

        if (this.liveClient()) {
            this.liveClient().dispose();
        }
    }
    
    attached() {
        super.attached();
        
        awesomeMultiselect.build($("#visibleTypesSelectorHttp"), this.getOptions(this.filteredTypeDataHttp));
        awesomeMultiselect.build($("#visibleTypesSelectorTcp"), this.getOptions(this.filteredTypeDataTcp));
    }
    
    private getOptions(filteredTypeData: typeData[]): (opts: any) => void {
        return (opts: any) => {
            opts.enableHTML = true;
            opts.includeSelectAllOption = true;
            opts.nSelectedText = " Types Selected";
            opts.allSelectedText = "All Types";
            opts.optionLabel = (element: HTMLOptionElement) => {
                const propertyName = $(element).text();
                const typeItem = filteredTypeData.find(x => x.propertyName === propertyName);
                const countClass = typeItem.count() ? "text-warning" : "";
                return `<span class="name">${generalUtils.escape(propertyName)}</span><span class="badge ${countClass}">${typeItem.count().toLocaleString()}</span>`;
            };
        }
    }

    private static syncMultiSelect() {
        awesomeMultiselect.rebuild($("#visibleTypesSelectorHttp"));
        awesomeMultiselect.rebuild($("#visibleTypesSelectorTcp"));
    }

    private refresh() {
        this.gridController().reset(false);
    }

    private matchesFilters(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase) {
        if (trafficWatch.isHttpItem(item)) {
            const textFilterLower = this.filter() ? this.filter().trim().toLowerCase() : "";
            const uri = item.RequestUri.toLocaleLowerCase();
            const customInfo = item.CustomInfo;

            const textFilterMatch = textFilterLower ? item.ResponseStatusCode.toString().includes(textFilterLower) ||
                item.DatabaseName.includes(textFilterLower)                   ||
                item.HttpMethod.toLocaleLowerCase().includes(textFilterLower) ||
                item.ClientIP.includes(textFilterLower)                       ||
                (customInfo && customInfo.toLocaleLowerCase().includes(textFilterLower)) ||
                uri.includes(textFilterLower): true;

            const typeMatch = _.includes(this.selectedTypeNamesHttp(), item.Type);
            const statusMatch = !this.onlyErrors() || item.ResponseStatusCode >= 400;

            return textFilterMatch && typeMatch && statusMatch;
        }
        
        if (trafficWatch.isTcpItem(item)) {
            const textFilterLower = this.filter() ? this.filter().trim().toLowerCase() : "";
            const details = trafficWatch.formatDetails(item).toLocaleLowerCase();
            const customInfo = item.CustomInfo;

            const textFilterMatch = textFilterLower ? details.includes(textFilterLower) || (customInfo && customInfo.toLocaleLowerCase().includes(textFilterLower)) : true;
            const operationMatch = _.includes(this.selectedTypeNamesTcp(), item.Operation);
            const statusMatch = !this.onlyErrors() || item.CustomInfo;

            return textFilterMatch && operationMatch && statusMatch;
        }
        
        return false;
    }
    
    private static isHttpItem(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase): item is Raven.Client.Documents.Changes.TrafficWatchHttpChange {
        return item.TrafficWatchType === "Http";
    }

    private static isTcpItem(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase): item is Raven.Client.Documents.Changes.TrafficWatchTcpChange {
        return item.TrafficWatchType === "Tcp";
    }

    private updateStats(): void {
        this.clientIps = [];
        
        this.filteredData.forEach(x => {
            if (!_.includes(this.clientIps, x.ClientIP)) {
                this.clientIps.push(x.ClientIP);
            }
        });
        
        this.filteredDataHttp = this.filteredData.filter(x => trafficWatch.isHttpItem(x))
            .map(x => x as Raven.Client.Documents.Changes.TrafficWatchHttpChange);
        
        this.filteredDataTcp = this.filteredData.filter(x => trafficWatch.isTcpItem(x))
            .map(x => x as Raven.Client.Documents.Changes.TrafficWatchTcpChange);

        this.stats.clientCount(this.clientIps.length.toLocaleString());
        this.stats.httpRequestCount(this.filteredDataHttp.length.toLocaleString());
        this.stats.tcpOperationCount(this.filteredDataTcp.length.toLocaleString());

        if (!this.filteredDataHttp.length) {
            this.httpStatsNotAvailable();
        } else {
            const filteredDataHttpNoWebSockets = this.filteredDataHttp.filter(x => x.ResponseStatusCode !== 101);
            
            if (filteredDataHttpNoWebSockets.length === 0) {
                this.httpStatsNotAvailable();
            } else {
                this.updateDurationStats(filteredDataHttpNoWebSockets);
                this.updatePercentiles(filteredDataHttpNoWebSockets);
            }
        }
    }
    
    private httpStatsNotAvailable(): void {
        this.stats.avg("N/A");
        this.stats.min("N/A");
        this.stats.max("N/A");

        this.stats.percentile_90("N/A");
        this.stats.percentile_99("N/A");
        this.stats.percentile_99_9("N/A");
    }

    private updateDurationStats(data: Raven.Client.Documents.Changes.TrafficWatchHttpChange[]): void {
        const firstItem = data[0];

        let sum = 0;
        let min = firstItem.ElapsedMilliseconds;
        let max = firstItem.ElapsedMilliseconds;

        for (let i = 0; i < data.length; i++) {
            const item = data[i];

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
        this.stats.avg(generalUtils.formatTimeSpan(sum / data.length));
    }
    
    private updatePercentiles(data: Raven.Client.Documents.Changes.TrafficWatchHttpChange[]): void {
        const timings = [] as number[];

        for (let i = data.length - 1; i >= 0; i--) {
            const item = data[i];

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

    private formatSource(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase, asHtml: boolean) {
        const thumbprint = item.CertificateThumbprint;
        const cert = thumbprint ? this.certificatesCache.get(thumbprint) : null;
        const certName = cert?.name;
        
        if (asHtml) {
            if (cert) {
                return (
                    `<div class="dataContainer dataContainer-lg">
                        <div>
                            <div class="dataLabel">Source: </div>
                            <div class="dataValue">${generalUtils.escapeHtml(item.ClientIP)}</div>
                        </div>
                        <div>
                            <div class="dataLabel">Certificate: </div>
                            <div class="dataValue">${generalUtils.escapeHtml(cert.name)}</div>
                        </div>
                        <div>
                            <div class="dataLabel">Thumbprint: </div>
                            <div class="dataValue">${generalUtils.escapeHtml(thumbprint)}</div>
                        </div>
                    </div>`);
            }
            return (
                `<div class="dataContainer">
                        <div>
                            <div class="dataLabel">Source: </div>
                            <div class="dataValue">${generalUtils.escapeHtml(item.ClientIP)}</div>
                        </div>
                    </div>`);
        } else {
            if (cert) {
                return "Source: " + item.ClientIP + ", Certificate Name = " + certName + ", Certificate Thumbprint = " + thumbprint;
            }
            return "Source: " + item.ClientIP;
        }
    }

    private static formatDetails(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase) {
        if (trafficWatch.isHttpItem(item)) {
            return item.RequestUri;
        }
        if (trafficWatch.isTcpItem(item)) {
            return item.Operation + (item.Source ? " from node " + item.Source : "") + (item.OperationVersion ? " (version " + item.OperationVersion + ")" : "");
        }

        return "N/A";
    }

    compositionComplete() {
        super.compositionComplete();

        $('.traffic-watch [data-toggle="tooltip"]').tooltip({
            html: true
        });

        const rowHighlightRules = (item: Raven.Client.Documents.Changes.TrafficWatchChangeBase) => {
            if (trafficWatch.isHttpItem(item)) {
                const responseCode = item.ResponseStatusCode.toString();
                if (responseCode.startsWith("4")) {
                    return "bg-warning";
                } else if (responseCode.startsWith("5")) {
                    return "bg-danger";
                }
            }
            
            if (trafficWatch.isTcpItem(item) && item.CustomInfo) {
                return "bg-warning";
            }
           
            return "";
        };
        
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init((s, t) => this.fetchTraffic(s, t), () =>
            [
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid, x => generalUtils.formatUtcDateAsLocal(x.TimeStamp, trafficWatch.dateTimeFormat), "Timestamp", "12%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid, x => `<span class="icon-info text-default"></span>`, "Src", "50px", {
                    extraClass: rowHighlightRules,
                    useRawValue: () => true
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(
                     grid, 
                    x => trafficWatch.isHttpItem(x) ? x.ResponseStatusCode : "N/A", 
                    "HTTP Status", 
                    "8%", 
                    {
                        extraClass: rowHighlightRules,
                        sortable: "number"
                    }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid, x => x.DatabaseName, "Database Name", "8%", {
                    extraClass: rowHighlightRules,
                    sortable: "string",
                    customComparator: generalUtils.sortAlphaNumeric
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(
                    grid,
                    x => trafficWatch.isHttpItem(x) ? x.ElapsedMilliseconds : "N/A",
                    "Duration",
                    "8%",
                    {
                        extraClass: rowHighlightRules,
                        sortable: "number"
                    }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(
                    grid,
                    x => trafficWatch.isHttpItem(x) ? x.HttpMethod : "TCP",
                    "Method",
                    "6%",
                    {
                        extraClass: rowHighlightRules,
                        sortable: "string"
                    }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(
                    grid,
                    x => trafficWatch.isHttpItem(x) ? x.Type : (trafficWatch.isTcpItem(x) ? x.Operation : "N/A"),
                    "Type",
                    "6%",
                    {
                        extraClass: rowHighlightRules,
                        sortable: "string"
                    }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid, x => x.CustomInfo, "Custom Info", "8%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(
                    grid,
                    x => trafficWatch.formatDetails(x),
                    "Details",
                    "35%",
                    {
                        extraClass: rowHighlightRules,
                        sortable: "string"
                    })
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-traffic-watch-tooltip",
            (item: Raven.Client.Documents.Changes.TrafficWatchChangeBase, column: textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                if (column.header === "Details") {
                    onValue(trafficWatch.formatDetails(item));
                } else if (column.header === "Timestamp") {
                    onValue(moment.utc(item.TimeStamp), item.TimeStamp);
                } else if (column.header === "Custom Info") {
                    onValue(generalUtils.escapeHtml(item.CustomInfo), item.CustomInfo);
                } else if (column.header === "Src") {
                    onValue(this.formatSource(item, true), this.formatSource(item, false));
                } else if (column.header === "Client IP") {
                    onValue(item.ClientIP);
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

    private fetchTraffic(skip: number, take: number): JQueryPromise<pagedResult<Raven.Client.Documents.Changes.TrafficWatchChangeBase>> {
        const textFilterDefined = this.filter();
        
        const filterUsingTypeHttp = this.selectedTypeNamesHttp().length !== this.filteredTypeDataHttp.length;
        const filterUsingTypeTcp = this.selectedTypeNamesTcp().length !== this.filteredTypeDataTcp.length;
        
        const filterUsingStatus = this.onlyErrors();
        
        if (textFilterDefined || filterUsingTypeHttp || filterUsingTypeTcp || filterUsingStatus) {
            this.filteredData = this.allData.filter(item => this.matchesFilters(item));
            this.isDataFiltered(true);
        } else {
            this.filteredData = this.allData;
            this.isDataFiltered(false);
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
        return this.liveClient() && this.liveClient().isConnected();
    }

    private onData(data: Raven.Client.Documents.Changes.TrafficWatchChangeBase) {
        if (this.allData.length === trafficWatch.maxBufferSize) {
            this.isBufferFull(true);
            this.pause();
            return;
        }
        
        this.allData.push(data);
        
        if (trafficWatch.isHttpItem(data)) {
            this.filteredTypeDataHttp.find(x => x.propertyName === data.Type).inc();
        } else if (trafficWatch.isTcpItem(data)) {
            this.filteredTypeDataTcp.find(x => x.propertyName === data.Operation).inc();
        }
        
        this.typesMultiSelectRefreshThrottle();

        if (!this.appendElementsTask) {
            this.appendElementsTask = setTimeout(() => this.onAppendPendingEntries(), 333);
        }
    }

    clearTypeCounter(): void {
        this.filteredTypeDataHttp.forEach(x => x.count(0));
        this.filteredTypeDataTcp.forEach(x => x.count(0));
        trafficWatch.syncMultiSelect();
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
        this.clientIps = [];
        
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
