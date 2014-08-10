import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import watchTrafficConfigDialog = require("viewmodels/watchTrafficConfigDialog");
import httpTraceClient = require("common/httpTraceClient");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import moment = require("moment");

class watchTraffic extends viewModelBase {
    logConfig = ko.observable<{ ResourceName:string; ResourcePath: string; MaxEntries: number; WatchedResourceMode: string; SingleAuthToken: singleAuthToken }>();
    traceClient: httpTraceClient;
    isConnected = ko.observable(false);
    recentEntries = ko.observableArray<any>([]);
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;
    selectedLog = ko.observable<logDto>();
    columnWidths: Array<KnockoutObservable<number>>;
    watchedRequests:number = 0;


    constructor() {
        super();
    }
    
    activate(args) {
        var widthUnit = 0.08;
        this.columnWidths = [
            ko.observable<number>(100 * widthUnit),
            ko.observable<number>(100 * widthUnit),
            ko.observable<number>(100 * widthUnit * 6),
            ko.observable<number>(100 * widthUnit * 2),
            ko.observable<number>(100 * widthUnit * 2)
        ];
        this.registerColumnResizing();    
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
        var configDialog = new watchTrafficConfigDialog();
        app.showDialog(configDialog);

        configDialog.configurationTask.done((x: any) => {
            this.logConfig(x);
            this.reconnect();
        });
    }

    reconnect() {
        if (!this.traceClient) {
            if (!this.logConfig) {
                app.showMessage("Cannot reconnect, please configure connection properly", "Connection Error");
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
        if (!!this.traceClient) {
            this.reconnect();
            return;
        }
        if (!this.logConfig()) {
            this.configureConnection();
            return;
        }

        var tokenDeferred = $.Deferred();

        if (!this.logConfig().SingleAuthToken) {
            new getSingleAuthTokenCommand(this.logConfig().ResourcePath, this.logConfig().WatchedResourceMode == "AdminView")
                .execute()
                .done((tokenObject: singleAuthToken) => {
                    this.logConfig().SingleAuthToken = tokenObject;
                    tokenDeferred.resolve();
                })
                .fail((e) => {
                    app.showMessage("You are not authorized to trace this resource", "Ahuthorization error");
                });
        } else {
            tokenDeferred.resolve();
        }

        tokenDeferred.done(() => {
            this.watchedRequests = 0;
            this.traceClient = new httpTraceClient(this.logConfig().ResourcePath, this.logConfig().SingleAuthToken.Token);
            this.traceClient.connect();
            this.traceClient.connectionOpeningTask.done(() => {
                this.isConnected(true);
                this.traceClient.watchTraffic((event: logNotificationDto) => {
                    this.processHttpTraceMessage(event);
                });
            });
            this.logConfig().SingleAuthToken = null;
        });


    }
    
    disconnect(): JQueryPromise<any> {
        if (!!this.traceClient) {
            this.traceClient.disconnect();
            return this.traceClient.connectionClosingTask.done(() => {
                this.traceClient = null;
                this.isConnected(false);
            });
        } else {
            app.showMessage("Cannot disconnet, connection does not exist", "Disconnect");
            return $.Deferred().reject();
        }
    }

    processHttpTraceMessage(e: logNotificationDto) {
        var logObject;
        logObject = {
            Time: this.createHumanReadableTime(e.TimeStamp, true, false),
            Duration: e.EllapsedMiliseconds,
            Resource: e.TenantName,
            Method: e.HttpMethod,
            Url: e.RequestUri,
            CustomInfo: e.CustomInfo,
            TimeStampText: this.createHumanReadableTime(e.TimeStamp, true, true)
//            Message: !e.CustomInfo ? this.formatLogRecord(e) : e.CustomInfo,
        };
        this.recentEntries.unshift(logObject);
        this.watchedRequests++;
    }


    selectLog(log: logDto) {
        this.selectedLog(log);
    }

    updateCurrentNowTime() {
        this.now(moment());
        if (this.updateNowTimeoutHandle != 0)
            clearTimeout(this.updateNowTimeoutHandle);
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 6000);
    }

    createHumanReadableTime(time: string, chainHumanized: boolean= true, chainDateTime: boolean= true): KnockoutComputed<string> {
        if (time) {
            return ko.computed(() => {
                var dateMoment = moment(time);
                var humanized = "", formattedDateTime = "";
                var agoInMs = dateMoment.diff(this.now());
                if (chainHumanized == true)
                    humanized = moment.duration(agoInMs).humanize(true);
                if (chainDateTime == true)
                    formattedDateTime = dateMoment.format(" (MM/DD/YY, h:mma)");
                return humanized + formattedDateTime;
            });
        }

        return ko.computed(() => time);
    }

    formatLogRecord(logRecord: logNotificationDto) {
        return 'Request #' + logRecord.RequestId.toString().paddingRight(' ', 4) + ' ' + logRecord.HttpMethod.paddingLeft(' ', 7) + ' - ' + logRecord.EllapsedMiliseconds.toString().paddingRight(' ', 5) + ' ms - ' + logRecord.TenantName.paddingLeft(' ', 10) + ' - ' + logRecord.ResponseStatusCode + ' - ' + logRecord.RequestUri;
    }
}

export =watchTraffic;