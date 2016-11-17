import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import watchTrafficConfigDialog = require("viewmodels/manage/watchTrafficConfigDialog");
import trafficWatchClient = require("common/trafficWatchClient");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import moment = require("moment");
import fileDownloader = require("common/fileDownloader");
import resource = require("models/resources/resource");
import enableQueryTimings = require("commands/database/query/enableQueryTimings");
import database = require("models/resources/database");
import accessHelper = require("viewmodels/shell/accessHelper");
import eventsCollector = require("common/eventsCollector");

class trafficWatch extends viewModelBase {
    logConfig = ko.observable<{ Resource: resource; ResourceName:string; ResourcePath: string; MaxEntries: number; WatchedResourceMode: string; SingleAuthToken: singleAuthToken }>();
    watchClient: trafficWatchClient;
    isConnected = ko.observable(false);
    recentEntries = ko.observableArray<any>([]);
    now = ko.observable<moment.Moment>();
    updateNowTimeoutHandle = 0;
    selectedLog = ko.observable<logDto>();
    columnWidths: Array<KnockoutObservable<number>>;
    keepDown = ko.observable(false);
    watchedRequests = ko.observable<number>(0);
    averageRequestDuration = ko.observable<string>();
    summedRequestsDuration:number=0;
    minRequestDuration = ko.observable<number>(1000000);
    maxRequestDuration = ko.observable<number>(0);
    startTraceTime = ko.observable<moment.Moment>();
    startTraceTimeHumanized :KnockoutComputed<string>;
    showLogDetails = ko.observable<boolean>(false);
    logRecordsElement: Element;
    isForbidden = ko.observable<boolean>();
    filter = ko.observable<string>();
    filterDuration = ko.observable<string>();

    enableTimingsTimer: number;

    constructor() {
        super();

        this.startTraceTimeHumanized = ko.computed(()=> {
            var a = this.now();
            if (!!this.startTraceTime()) {
                return this.parseHumanReadableTimeString(this.startTraceTime().toString(), true, false);
            }
            return "";
        });

        this.isForbidden(accessHelper.isGlobalAdmin() === false);
        this.filter.throttle(250).subscribe(() => this.filterEntries());
        this.filterDuration.throttle(250).subscribe(() => this.filterEntries());
    }

    filterEntries() {
        this.recentEntries().forEach(entry => {
            entry.Visible(this.isVisible(entry));
        });
    }

    canActivate(args:any): any {
        return true;
    }
    
    activate(args: any) {
        var widthUnit = 0.075;
        this.columnWidths = [
            ko.observable<number>(100 * widthUnit),
            ko.observable<number>(100 * widthUnit),
            ko.observable<number>(100 * widthUnit ),
            ko.observable<number>(100 * widthUnit),
            ko.observable<number>(100 * widthUnit * 7),
            ko.observable<number>(100 * widthUnit)
        ];
        this.registerColumnResizing();    
        this.updateHelpLink('EVEP6I');
    }

    attached() {
        super.attached();
        this.showLogDetails.subscribe(x => {
                $(".logRecords").toggleClass("logRecords-small");
        });
        this.updateCurrentNowTime();
        this.logRecordsElement = document.getElementById("logRecords");
    }

    registerColumnResizing() {
        var resizingColumn = false;
        var startX = 0;
        var startingWidth = 0;
        var columnIndex = 0;

        $(document).on("mousedown.logTableColumnResize", ".column-handle", (e: any) => {
            columnIndex = parseInt($(e.currentTarget).attr("column"));
            startingWidth = this.columnWidths[columnIndex]();
            startX = e.pageX;
            resizingColumn = true;
        });

        $(document).on("mouseup.logTableColumnResize", "", (e: any) => {
            resizingColumn = false;
        });

        $(document).on("mousemove.logTableColumnResize", "", (e: any) => {
            if (resizingColumn) {
                var logsRecordsContainerWidth = $("#logRecordsContainer").width();
                var targetColumnSize = startingWidth + 100 * (e.pageX - startX) / logsRecordsContainerWidth;
                this.columnWidths[columnIndex](targetColumnSize);

                // Stop propagation of the event so the text selection doesn't fire up
                if (e.stopPropagation) e.stopPropagation();
                if (e.preventDefault) e.preventDefault();
                e.cancelBubble = true;
                e.returnValue = false;

                return false;
            }
        });
    }

    configureConnection() {
        eventsCollector.default.reportEvent("traffic-watch", "configure");

        var configDialog = new watchTrafficConfigDialog();
        app.showBootstrapDialog(configDialog);

        configDialog.configurationTask.done((x: any) => {
            this.logConfig(x);
            this.enableTimingsTimer = setInterval(() => this.enableQueryTiming(), 4.8 * 60 * 1000);
            this.enableQueryTiming();
            this.reconnect();
        });
    }

    enableQueryTiming() {
        if (this.logConfig().Resource instanceof database) {
            new enableQueryTimings(<database>this.logConfig().Resource).execute();
        }
    }

    reconnect() {
        eventsCollector.default.reportEvent("traffic-watch", "reconnect");

        if (!this.watchClient) {
            if (!this.logConfig) {
                app.showBootstrapMessage("Cannot reconnect, please configure connection properly", "Connection Error");
                return;
            }
            this.connect();
        } else {
            this.disconnect().done(() => {
                this.connect();
            });
        }
    }

    connect() {
        eventsCollector.default.reportEvent("traffic-watch", "connect");

        if (!!this.watchClient) {
            this.reconnect();
            return;
        }
        if (!this.logConfig()) {
            this.configureConnection();
            return;
        }

        var tokenDeferred = $.Deferred();

        if (!this.logConfig().SingleAuthToken) {
            new getSingleAuthTokenCommand(this.logConfig().Resource, this.logConfig().WatchedResourceMode === "AdminView")
                .execute()
                .done((tokenObject: singleAuthToken) => {
                    this.logConfig().SingleAuthToken = tokenObject;
                    tokenDeferred.resolve();
                })
                .fail((e) => {
                    app.showBootstrapMessage("You are not authorized to trace this resource", "Authorization error");
                });
        } else {
            tokenDeferred.resolve();
        }

        tokenDeferred.done(() => {
            this.watchClient = new trafficWatchClient(this.logConfig().ResourcePath, this.logConfig().SingleAuthToken.Token);
            this.watchClient.connect();
            this.watchClient.connectionOpeningTask.done(() => {
                this.isConnected(true);
                this.watchClient.watchTraffic((event: logNotificationDto) => {
                    this.processHttpTraceMessage(event);
                });
                if (!this.startTraceTime()) {
                    this.startTraceTime(this.now());
                }
            });
            this.logConfig().SingleAuthToken = null;
        });
    }
    
    disconnect(): JQueryPromise<any> {
        eventsCollector.default.reportEvent("traffic-watch", "disconnect");

        if (!!this.watchClient) {
            this.watchClient.disconnect();
            return this.watchClient.connectionClosingTask.done(() => {
                this.watchClient = null;
                this.isConnected(false);
            });
        } else {
            app.showBootstrapMessage("Cannot disconnect, connection does not exist", "Disconnect");
            return $.Deferred().reject();
        }
    }

    deactivate() {
        super.deactivate();
        if (this.enableTimingsTimer) {
            clearInterval(this.enableTimingsTimer);
            this.enableTimingsTimer = null;
        }
        if (this.isConnected())
            this.disconnect();
    }

    isVisible(logEntry: any) {
        if (this.filterDuration() && logEntry.Duration < parseInt(this.filterDuration())) {
            return false;
        }

        if (this.filter() && logEntry.Url.indexOf(this.filter()) === -1) {
            return false;
        }
        return true;
    }

    processHttpTraceMessage(e: logNotificationDto) {
        var logObject: any;
        
        var mapTimings = (value: any) => {
            var result: any = [];
            if (!value) {
                return result;
            }
            for (var key in value) {
                if (value.hasOwnProperty(key)) {
                    result.push({ key: key, value: value[key] });
                }
            }
            return result;

        }

        logObject = {
            Time: this.createHumanReadableTime(e.TimeStamp, false, true),
            Duration: e.ElapsedMilliseconds,
            Resource: e.TenantName,
            Method: e.HttpMethod,
            Url: e.RequestUri,
            CustomInfo: e.CustomInfo,
            TimeStampText: this.createHumanReadableTime(e.TimeStamp, true, false),
            QueryTimings: mapTimings(e.QueryTimings),
            Visible: ko.observable()
        };

        if (logObject.CustomInfo) {
            logObject.CustomInfo = decodeURIComponent(logObject.CustomInfo).replaceAll("\n", "<Br />").replaceAll("Inner Request", "<strong>Inner Request</strong>");
        }

        if (e.InnerRequestsCount > 0) {
            logObject.Url = "(" + e.InnerRequestsCount + " requests) " + logObject.Url;
        }

        if (this.recentEntries().length == this.logConfig().MaxEntries) {
            this.recentEntries.shift();
        }

        logObject.Visible(this.isVisible(logObject));

        this.recentEntries.push(logObject);

        if (this.keepDown()) {
            this.logRecordsElement.scrollTop = this.logRecordsElement.scrollHeight * 1.1;
        }

        this.watchedRequests(this.watchedRequests() + 1);
        this.summedRequestsDuration += e.ElapsedMilliseconds;
        this.averageRequestDuration((this.summedRequestsDuration / this.watchedRequests()).toFixed(2));
        this.minRequestDuration(this.minRequestDuration() > e.ElapsedMilliseconds ? e.ElapsedMilliseconds : this.minRequestDuration());
        this.maxRequestDuration(this.maxRequestDuration() < e.ElapsedMilliseconds ? e.ElapsedMilliseconds : this.maxRequestDuration());
    }


    selectLog(log: logDto) {
        this.selectedLog(log);
        this.showLogDetails(true);
        $(".logRecords").addClass("logRecords-small");
    }

    updateCurrentNowTime() {
        this.now(moment());
        if (this.updateNowTimeoutHandle != 0)
            clearTimeout(this.updateNowTimeoutHandle);
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 1000);
    }

    createHumanReadableTime(time: string, chainHumanized: boolean = true, chainDateTime: boolean= true): KnockoutComputed<string> {
        if (time) {
            return ko.computed(() => {
                return this.parseHumanReadableTimeString(time, chainHumanized, chainDateTime);
            });
        }

        return ko.computed(() => time);
    }

    parseHumanReadableTimeString(time: string, chainHumanized: boolean= true, chainDateTime: boolean= true)
{
        var dateMoment = moment(time);
        var humanized = "", formattedDateTime = "";
        var agoInMs = dateMoment.diff(this.now());
        if (chainHumanized)
            humanized = moment.duration(agoInMs).humanize(true);
        if (chainDateTime)
            formattedDateTime = dateMoment.format(" (ddd MMM DD YYYY HH:mm:ss.SS[GMT]ZZ)");
        return humanized + formattedDateTime;
}

    formatLogRecord(logRecord: logNotificationDto) {
        return 'Request #' + logRecord.RequestId.toString().paddingRight(' ', 4) + ' ' + logRecord.HttpMethod.paddingLeft(' ', 7) + ' - ' + logRecord.ElapsedMilliseconds.toString().paddingRight(' ', 5) + ' ms - ' + logRecord.TenantName.paddingLeft(' ', 10) + ' - ' + logRecord.ResponseStatusCode + ' - ' + logRecord.RequestUri;
    }

    resetStats() {
        eventsCollector.default.reportEvent("traffic-watch", "reset-stats");

        this.watchedRequests(0);
        this.averageRequestDuration("0");
        this.summedRequestsDuration = 0;
        this.minRequestDuration(1000000);
        this.maxRequestDuration(0);
        this.startTraceTime(null);
    }

    exportTraffic() {
        eventsCollector.default.reportEvent("traffic-watch", "export");

        fileDownloader.downloadAsJson(this.recentEntries(), "traffic.json");
    }

    clearLogs() {
        eventsCollector.default.reportEvent("traffic-watch", "clear");

        this.recentEntries.removeAll();
    }

    toggleKeepDown() {
        eventsCollector.default.reportEvent("traffic-watch", "keep-down");

        this.keepDown.toggle();
        if (this.keepDown()) {
            this.logRecordsElement.scrollTop = this.logRecordsElement.scrollHeight * 1.1;
        }
    }
}

export =trafficWatch;
