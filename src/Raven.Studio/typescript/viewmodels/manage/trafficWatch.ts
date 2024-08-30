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
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import recentQueriesStorage = require("common/storage/savedQueriesStorage");
import queryCriteria = require("models/database/query/queryCriteria");
import databasesManager = require("common/shell/databasesManager");
import accessManager = require("common/shell/accessManager");
import TrafficWatchHttpChange = Raven.Client.Documents.Changes.TrafficWatchHttpChange;
import trafficWatchQueriesDialog from "viewmodels/manage/trafficWatchQueriesDialog";
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import queryTimingsDialog from "viewmodels/manage/queryTimingsDialog";

type trafficChangeType =
    Raven.Client.Documents.Changes.TrafficWatchChangeType
    | Raven.Client.ServerWide.Tcp.TcpConnectionHeaderMessage.OperationTypes;


class showTimingsFeature implements columnPreviewFeature {
    install($tooltip: JQuery, valueProvider: () => any, elementProvider: () => Raven.Client.Documents.Changes.TrafficWatchHttpChange) {
        $tooltip.on("click", ".show-timings", () => {
            const item = elementProvider();
            
            app.showBootstrapDialog(new queryTimingsDialog(item.QueryTimings, item.CustomInfo));
        });
    }
    
    syntax(column: virtualColumn, escapedValue: any, element: Raven.Client.Documents.Changes.TrafficWatchChangeBase) {
        console.log(column, escapedValue, element);
        if (column.header !== "Duration" || escapedValue === generalUtils.escapeHtml("n/a")) {
            return "";
        }

        if (!trafficWatch.isHttpItem(element) || !element.QueryTimings) {
            return "";
        }

        return `<button class="btn btn-default btn-sm show-timings"><i class="icon-stats"></i><span>Show timings</span></button>`;
    }
}

class runQueryFeature implements columnPreviewFeature {

    private queryList: string[] = [];

    install($tooltip: JQuery, valueProvider: () => any, elementProvider: () => any): void {
        $tooltip.on("click", ".run-query", () => {
            const value = valueProvider();
            const item: Raven.Client.Documents.Changes.TrafficWatchChangeBase = elementProvider();

            if (item.TrafficWatchType !== "Http" || (item as TrafficWatchHttpChange).Type !== "MultiGet") {
                runQueryFeature.executeQuery(value, item);
                return;
            }

            const queryList = this.queryList;
            
            if (queryList.length === 1) {
                runQueryFeature.executeQuery(queryList[0], item);
            } else {
                app.showBootstrapDialog(new trafficWatchQueriesDialog(queryList))
                    .done(queryToExecute => {
                        if (queryToExecute) {
                            runQueryFeature.executeQuery(queryToExecute, item);
                        }
                    });
            }
        });
    }

    private static getMultiGetQueriesList(value: any): string[] {
        const queryList: string[] = [];

        const lines = value.split(/\r?\n/);
        
        lines.forEach((line: string) => {
            if (line) {
                const jsonObj = JSON.parse(line);

                const queriesEndpoint = jsonObj.Url?.endsWith("/queries");
                const query = jsonObj.Query?.slice("?query=".length);

                if (queriesEndpoint && query) {
                    queryList.push(query);
                }
            }
        });

        return queryList;
    }

    private static executeQuery(value: any, item: Raven.Client.Documents.Changes.TrafficWatchChangeBase): void {
        const query = queryCriteria.empty();

        query.queryText(value);
        query.name("Traffic watch query");
        query.recentQuery(true);

        const queryDto = query.toStorageDto();

        const db = databasesManager.default.getDatabaseByName(item.DatabaseName);

        recentQueriesStorage.saveAndNavigate(db, queryDto, { newWindow: true });
    }
    
    syntax(column: virtualColumn, escapedValue: any, element: any): string {
        if (column.header !== "Custom Info" || escapedValue === generalUtils.escapeHtml("n/a")) {
            return "";
        }
        
        this.queryList = [];
        let buttonText: string;

        if (element.Type === "MultiGet") {
            this.queryList = runQueryFeature.getMultiGetQueriesList(generalUtils.unescapeHtml(escapedValue));
            buttonText = this.queryList.length > 1 ? "Run Query ..." : (this.queryList.length === 1 ? "Run Query" : "");
        }        
        
        if (element.Type === "Queries" && (element.HttpMethod === "POST" || element.HttpMethod === "GET")) {
            buttonText = "Run Query";
        }
        
        return buttonText ? `<button class="btn btn-default btn-sm run-query"><i class="icon-query"></i><span>${buttonText}</span></button>` : "";
    }
}

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

interface statistics {
    min: string;
    avg: string;
    max: string;
    percentile_90: string;
    percentile_99: string;
    percentile_99_9: string;
}

const notAvailableStats: statistics = {
    min: "n/a",
    avg: "n/a",
    percentile_90: "n/a",
    percentile_99: "n/a",
    percentile_99_9: "n/a",
    max: "n/a"
}

class trafficWatch extends viewModelBase {
    
    view = require("views/manage/trafficWatch.html");
    
    static readonly usingHttps = location.protocol === "https:";
    static readonly isSecureServer = accessManager.default.secureServer();
    
    static maxBufferSize = 200000;
    
    static dateTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    private liveClient = ko.observable<trafficWatchWebSocketClient>();
    private allData: Raven.Client.Documents.Changes.TrafficWatchChangeBase[] = [];
    
    private filteredData: Raven.Client.Documents.Changes.TrafficWatchChangeBase[] = [];
    private filteredDataHttp: Raven.Client.Documents.Changes.TrafficWatchHttpChange[] = [];
    private filteredDataTcp: Raven.Client.Documents.Changes.TrafficWatchTcpChange[] = [];

    private sourceIps: string[] = [];

    certificatesCache = new Map<string, certificateInfo>();

    private gridController = ko.observable<virtualGridController<Raven.Client.Documents.Changes.TrafficWatchChangeBase>>();
    private columnPreview = new columnPreviewPlugin<Raven.Client.Documents.Changes.TrafficWatchChangeBase>();

    private readonly allTypeDataHttp: Raven.Client.Documents.Changes.TrafficWatchChangeType[] =
        ["BulkDocs", "ClusterCommands", "Counters", "Documents", "Hilo", "Index", "MultiGet", "None", "Notifications", "Operations", "Queries", "Streams", "Subscriptions", "TimeSeries"];
    private readonly allTypeDataTcp: Raven.Client.ServerWide.Tcp.TcpConnectionHeaderMessage.OperationTypes[] =
        ["Cluster", "Drop", "Heartbeats", "None", "Ping", "Replication", "Subscription", "TestConnection"];
    
    private filteredTypeDataHttp = this.allTypeDataHttp.map(x => new typeData(x));
    private selectedTypeNamesHttp = ko.observableArray<string>(this.allTypeDataHttp.splice(0));
    
    private filteredTypeDataTcp = this.allTypeDataTcp.map(x => new typeData(x));
    private selectedTypeNamesTcp = ko.observableArray<string>(this.allTypeDataTcp.splice(0));
    
    onlyErrors = ko.observable<boolean>(false);

    adminLogsUrl = appUrl.forAdminLogs() + "?highlightTrafficWatch=true"

    stats = {
        sourceIpsCount: ko.observable<string>(),
        httpRequestCount: ko.observable<string>(),
        tcpOperationCount: ko.observable<string>(),
        requestDuration: ko.observable<statistics>(notAvailableStats),
        responseSize: ko.observable<statistics>(notAvailableStats)
    };
    
    filter = ko.observable<string>();
    isDataFiltered = ko.observable<boolean>(false);
    
    private appendElementsTask: ReturnType<typeof setTimeout>;

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

        if (trafficWatch.isSecureServer) {
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

    private refresh(): void {
        this.gridController().reset(false);
    }

    private matchesFilters(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase) {
        if (trafficWatch.isHttpItem(item)) {
            const textFilterLower = this.filter() ? this.filter().trim().toLowerCase() : "";
            const uri = item.RequestUri.toLocaleLowerCase();
            const customInfo = item.CustomInfo;

            const textFilterMatch = textFilterLower ? item.ResponseStatusCode.toString().includes(textFilterLower)  ||
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
    
    static isHttpItem(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase): item is Raven.Client.Documents.Changes.TrafficWatchHttpChange {
        return item.TrafficWatchType === "Http";
    }

    private static isTcpItem(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase): item is Raven.Client.Documents.Changes.TrafficWatchTcpChange {
        return item.TrafficWatchType === "Tcp";
    }

    private updateStats(): void {
        this.sourceIps = [];
        
        this.filteredData.forEach(x => {
            if (!_.includes(this.sourceIps, x.ClientIP)) {
                this.sourceIps.push(x.ClientIP);
            }
        });
        
        this.filteredDataHttp = this.filteredData.filter(x => trafficWatch.isHttpItem(x))
            .map(x => x as Raven.Client.Documents.Changes.TrafficWatchHttpChange);
        
        this.filteredDataTcp = this.filteredData.filter(x => trafficWatch.isTcpItem(x))
            .map(x => x as Raven.Client.Documents.Changes.TrafficWatchTcpChange);

        this.stats.sourceIpsCount(this.sourceIps.length.toLocaleString());
        this.stats.httpRequestCount(this.filteredDataHttp.length.toLocaleString());
        this.stats.tcpOperationCount(this.filteredDataTcp.length.toLocaleString());

        const filteredDataHttpNoWebSockets = this.filteredTypeDataHttp.length ? this.filteredDataHttp.filter(x => x.ResponseStatusCode !== 101) : [];
            
        this.stats.requestDuration(trafficWatch.computeStats(filteredDataHttpNoWebSockets, x => x.ElapsedMilliseconds, x => generalUtils.formatTimeSpan(x, false)));
        this.stats.responseSize(trafficWatch.computeStats(filteredDataHttpNoWebSockets, x => x.ResponseSizeInBytes, x => generalUtils.formatBytesToSize(x, 1)));
            }
                
    private static computeStats(data: TrafficWatchHttpChange[],
                                accessor: (item: TrafficWatchHttpChange) => number,
                                formatter: (value: number) => string): statistics {
        if (data.length === 0) {
            return notAvailableStats;
                }

        const scalars: number[] = [];
        let min = accessor(data[0]);
        let max = accessor(data[0]);
        let sum = 0;

        for (let i = data.length - 1; i >= 0; i--) {
            const item = data[i];
            const value = accessor(item);

            if (scalars.length === 2048) {
                // compute using max 2048 latest values
                break;
                }

            if (value < min) {
                min = value;
                }

            if (value > max) {
                max = value;
            }

            sum += value;

            scalars.push(value);
            }
    
        scalars.sort((a, b) => a - b);

        return {
            min: formatter(min),
            avg: formatter(sum / scalars.length),
            percentile_90: formatter(scalars[Math.ceil(90 / 100 * scalars.length) - 1]),
            percentile_99: formatter(scalars[Math.ceil(99 / 100 * scalars.length) - 1]),
            percentile_99_9: formatter(scalars[Math.ceil(99.9 / 100 * scalars.length) - 1]),
            max: formatter(max)
            }
        }

    private formatSource(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase, asHtml: boolean): string {
        const thumbprint = item.CertificateThumbprint;
        const cert = thumbprint ? this.certificatesCache.get(thumbprint) : null;
        const certName = cert?.name;
        
        if (asHtml) {
            if (cert) {
                return (
                    `<div class="data-container data-container-lg">
                        <div>
                            <div class="data-label">Source: </div>
                            <div class="data-value">${generalUtils.escapeHtml(item.ClientIP)}</div>
                        </div>
                        <div>
                            <div class="data-label">Certificate: </div>
                            <div class="data-value">${generalUtils.escapeHtml(cert.name)}</div>
                        </div>
                        <div>
                            <div class="data-label">Thumbprint: </div>
                            <div class="data-value">${generalUtils.escapeHtml(thumbprint)}</div>
                        </div>
                    </div>`);
            }
            return (
                `<div class="data-container">
                     <div>
                        <div class="data-label">Source: </div>
                        <div class="data-value">${generalUtils.escapeHtml(item.ClientIP)}</div>
                     </div>
                 </div>`);
        } else {
            if (cert) {
                return `Source: ${item.ClientIP}, Certificate Name = ${generalUtils.escapeHtml(certName)}, Certificate Thumbprint = ${thumbprint}`;
            }
            return `Source: ${item.ClientIP}`;
        }
    }

    private static formatDetails(item: Raven.Client.Documents.Changes.TrafficWatchChangeBase): string {
        if (trafficWatch.isHttpItem(item)) {
            return item.RequestUri;
        }
        if (trafficWatch.isTcpItem(item)) {
            return item.Operation + (item.Source ? " from node " + item.Source : "") + (item.OperationVersion ? " (version " + item.OperationVersion + ")" : "");
        }

        return "n/a";
    }

    compositionComplete() {
        super.compositionComplete();

        awesomeMultiselect.build($("#visibleTypesSelectorHttp"), this.getOptions(this.filteredTypeDataHttp));
        awesomeMultiselect.build($("#visibleTypesSelectorTcp"), this.getOptions(this.filteredTypeDataTcp));

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
        
        const durationProvider = (item: Raven.Client.Documents.Changes.TrafficWatchChangeBase) => {
            if (trafficWatch.isHttpItem(item)) { 
                const timingsPart = item.QueryTimings ? `<span class="icon-stats text-info margin-right margin-right-xs"></span>` : ""; 
                return item.ElapsedMilliseconds.toLocaleString() + " " + timingsPart;
            } else {
                return "n/a";
            }
        }
        
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(() => this.fetchTraffic(), () =>
            [
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => generalUtils.formatUtcDateAsLocal(x.TimeStamp, trafficWatch.dateTimeFormat),
                    "Timestamp", "12%", {
                    extraClass: rowHighlightRules,
                    sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => trafficWatch.isSecureServer ? `<span class="icon-certificate text-info margin-right margin-right-xs"></span>${x.ClientIP}` : x.ClientIP,
                    "Source", "8%", {
                    extraClass: rowHighlightRules,
                    useRawValue: () => true
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid, 
                    x => trafficWatch.isHttpItem(x) ? x.ResponseStatusCode : "n/a",
                    "HTTP Status", "8%", {
                        extraClass: rowHighlightRules,
                        sortable: "number"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => trafficWatch.isHttpItem(x) ? x.RequestSizeInBytes : null,
                    "Request Size", "8%", {
                        extraClass: rowHighlightRules,
                        sortable: "number",
                        transformValue: generalUtils.formatBytesToSize
                    }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => trafficWatch.isHttpItem(x) ? x.ResponseSizeInBytes : null,
                    "Response Size", "8%", {
                        extraClass: rowHighlightRules,
                        sortable: "number",
                        transformValue: generalUtils.formatBytesToSize
                    }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => x.DatabaseName,
                    "Database Name", "10%", {
                        extraClass: rowHighlightRules,
                        sortable: "string",
                        customComparator: generalUtils.sortAlphaNumeric
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => trafficWatch.isHttpItem(x) ? x.ElapsedMilliseconds : null,
                    "Duration", "6%", {
                        extraClass: rowHighlightRules,
                        sortable: "number", 
                        transformValue: (v, item) => durationProvider(item),
                        useRawValue: () => true
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => trafficWatch.isHttpItem(x) ? x.HttpMethod : "TCP",
                    "Method", "6%", {
                        extraClass: rowHighlightRules,
                        sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => trafficWatch.isHttpItem(x) ? x.Type : (trafficWatch.isTcpItem(x) ? x.Operation : "n/a"),
                    "Type", "6%", {
                        extraClass: rowHighlightRules,
                        sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => x.CustomInfo,
                    "Custom Info", "8%", {
                        extraClass: rowHighlightRules,
                        sortable: "string"
                }),
                new textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>(grid,
                    x => trafficWatch.formatDetails(x),
                    "Details", "20%", {
                        extraClass: rowHighlightRules,
                        sortable: "string"
                })
            ]
        );

        const runQuery = new runQueryFeature();
        const showTimings = new showTimingsFeature();
        
        this.columnPreview.install("virtual-grid", ".js-traffic-watch-tooltip",
            (item: Raven.Client.Documents.Changes.TrafficWatchChangeBase, column: textColumn<Raven.Client.Documents.Changes.TrafficWatchChangeBase>,
             e: JQuery.TriggeredEvent, onValue: (value: any, valueToCopy?: string, wrapValue?: boolean) => void) => {
                if (column.header === "Duration") {
                    onValue(trafficWatch.isHttpItem(item) ? item.ElapsedMilliseconds.toLocaleString() + " ms" : "n/a");
                } else if (column.header === "Details") {
                    onValue(trafficWatch.formatDetails(item));
                } else if (column.header === "Timestamp") {
                    onValue(moment.utc(item.TimeStamp), item.TimeStamp);
                } else if (column.header === "Custom Info") {
                    onValue(generalUtils.escapeHtml(item.CustomInfo), item.CustomInfo);
                } else if (column.header === "Source") {
                    onValue(this.formatSource(item, true), this.formatSource(item, false), false);
                }
            }, {
                additionalFeatures: [runQuery, showTimings]
            });

        $(".traffic-watch .viewport").on("scroll", () => {
            if (!this.duringManualScrollEvent && this.tailEnabled()) {
                this.tailEnabled(false);
            }

            this.duringManualScrollEvent = false;
        });
        
        this.connectWebSocket();
    }

    private fetchTraffic(): JQueryPromise<pagedResult<Raven.Client.Documents.Changes.TrafficWatchChangeBase>> {
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
        this.sourceIps = [];
        
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
