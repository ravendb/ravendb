import app = require("durandal/app");
import getLogsCommand = require("commands/getLogsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import onDemandLogsConfigureCommand = require("commands/onDemandLogsConfigureCommand");
import database = require("models/database");
import moment = require("moment");
import appUrl = require("common/appUrl");
import httpTraceClient = require("common/httpTraceClient");
import changeSubscription = require('models/changeSubscription');
import copyDocuments = require("viewmodels/copyDocuments");
import fileDownloader = require("common/fileDownloader");
import document = require("models/document");
import customLogging = require("viewmodels/customLogging");
import customLogConfig = require("models/customLogConfig");
import customLogEntry = require("models/customLogEntry");

import onDemandLogs = require("common/onDemandLogs");

class logs extends viewModelBase {

    onDemandLogging = ko.observable<onDemandLogs>(null);
    allLogs = ko.observableArray<logDto>();
    pendingLogs: logDto[] = [];
    rawLogs = ko.observable<logDto[]>([]);
    intervalId: number;
    maxEntries = ko.observable(0);
    filterLevel = ko.observable("All");
    selectedLog = ko.observable<logDto>();
    debugLogCount: KnockoutComputed<number>;
    infoLogCount: KnockoutComputed<number>;
    warningLogCount: KnockoutComputed<number>;
    errorLogCount: KnockoutComputed<number>;
    fatalLogCount: KnockoutComputed<number>;
    customLoggingEnabled: KnockoutComputed<boolean>;
    customLoggingInProgress = ko.observable(false);
    searchText = ko.observable("");
    searchTextThrottled: KnockoutObservable<string>;
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;
    filteredLoggers = ko.observableArray<string>();
    sortColumn = ko.observable<string>("TimeStamp");
    sortAsc = ko.observable<boolean>(true);
    filteredAndSortedLogs: KnockoutComputed<Array<logDto>>;
    columnWidths: Array<KnockoutObservable<number>>;
    logsMode = ko.observable<string>("Regular");
    logHttpTraceClient: httpTraceClient;
    httpTraceSubscription: changeSubscription;

    constructor() {
        super();

        this.debugLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Debug"));
        this.infoLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Info"));
        this.warningLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Warn"));
        this.errorLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Error"));
        this.fatalLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Fatal"));
        this.searchTextThrottled = this.searchText.throttle(400);
        this.activeDatabase.subscribe(() => this.fetchLogs());
        this.updateCurrentNowTime();

        this.filteredAndSortedLogs = ko.computed<Array<logDto>>(() => {
            var logs = this.allLogs();
            var column = this.sortColumn();
            var asc = this.sortAsc();

            var sortFunc = (left, right) => {
                if (left[column] === right[column]) { return 0; }
                var test = asc ? ((l, r) => l < r) : ((l, r) => l > r);
                return test(left[column], right[column]) ? 1 : -1;
            }

            return logs.sort(sortFunc);
        });
    }

    activate(args) {
        super.activate(args);
        this.columnWidths = [
            ko.observable<number>(100),
            ko.observable<number>(265),
            ko.observable<number>(300),
            ko.observable<number>(200),
            ko.observable<number>(360)
        ];
        this.registerColumnResizing();
        return this.fetchLogs();
    }

    attached() {
        var logsRecordsContainerWidth = $("#logRecordsContainer").width();
        var widthUnit = 0.08;
        this.columnWidths[0](100 * widthUnit);
        this.columnWidths[1](100 * widthUnit);
        this.columnWidths[2](100 * widthUnit * 6);
        this.columnWidths[3](100 * widthUnit * 2);
        this.columnWidths[4](100 * widthUnit * 2);
    }

    redraw() {
        if (this.pendingLogs.length > 0) {
            var pendingCopy = this.pendingLogs;
            this.pendingLogs = [];
            var logsAsText = "";
            pendingCopy.forEach(log => {
                var line = log.TimeStamp + ";" + log.Level.toUpperCase() + ";" + log.LoggerName + ";" + log.Message + (log.Exception || "") + "\n";
                logsAsText += line;
            });
            $("#rawLogsContainer pre").append(logsAsText); 
            this.rawLogs().pushAll(pendingCopy);  
            this.rawLogs.valueHasMutated();
        }
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
        clearInterval(this.intervalId);
        this.unregisterColumnResizing();
        if (this.onDemandLogging()) {
            this.onDemandLogging().dispose();
        }
    }

    fetchLogs(): JQueryPromise<logDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getLogsCommand(db)
                .execute()
                .done((results: logDto[]) => this.processLogResults(results));
        }

        return null;
    }

    processLogResults(results: logDto[], append:boolean=false) {
        var now = moment();
        results.forEach(r => {
            r['HumanizedTimestamp'] = this.createHumanReadableTime(r.TimeStamp,true,false);
            r['TimeStampText'] = this.createHumanReadableTime(r.TimeStamp,true,true);
            r['IsVisible'] = ko.computed(() => this.matchesFilterAndSearch(r) && !this.filteredLoggers.contains(r.LoggerName));
        });

        if (append === false) {
            this.allLogs(results.reverse());
        } else {
            if (results.length == 1) {
                this.allLogs.unshift((results[0]));
            } else {
                results.forEach(x=>this.allLogs.unshift(x));
            }
            
        }
    }

    matchesFilterAndSearch(log: logDto) {
        var searchTextThrottled = this.searchTextThrottled().toLowerCase();
        var filterLevel = this.filterLevel();
        var matchesLogLevel = filterLevel === "All" || log.Level === filterLevel;
        var matchesSearchText = !searchTextThrottled ||
            (log.Message && log.Message.toLowerCase().indexOf(searchTextThrottled) >= 0) ||
            (log.Exception && log.Exception.toLowerCase().indexOf(searchTextThrottled) >= 0);

        return matchesLogLevel && matchesSearchText;
    }

    createHumanReadableTime(time: string, chainHumanized: boolean= true, chainDateTime:boolean=true): KnockoutComputed<string> {
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

    selectLog(log: logDto) {
        this.selectedLog(log);
    }

    tableKeyDown(sender: any, e: KeyboardEvent) {
        var isKeyUp = e.keyCode === 38;
        var isKeyDown = e.keyCode === 40;
        if (isKeyUp || isKeyDown) {
            e.preventDefault();

            var oldSelection = this.selectedLog();
            if (oldSelection) {
                var oldSelectionIndex = this.allLogs.indexOf(oldSelection);
                var newSelectionIndex = oldSelectionIndex;
                if (isKeyUp && oldSelectionIndex > 0) {
                    newSelectionIndex--;
                } else if (isKeyDown && oldSelectionIndex < this.allLogs().length - 1) {
                    newSelectionIndex++;
                }

                this.selectedLog(this.allLogs()[newSelectionIndex]);
                var newSelectedRow = $("#logsContainer table tbody tr:nth-child(" + (newSelectionIndex + 1) + ")");
                if (newSelectedRow) {
                    this.ensureRowVisible(newSelectedRow);
                }
            }
        }
    }

    showContextMenu() {
        //alert("this");
    }

    ensureRowVisible(row: JQuery) {
        var table = $("#logTableContainer");
        var scrollTop = table.scrollTop();
        var scrollBottom = scrollTop + table.height();
        var scrollHeight = scrollBottom - scrollTop;

        var rowPosition = row.position();
        var rowTop = rowPosition.top;
        var rowBottom = rowTop + row.height();

        if (rowTop < 0) {
            table.scrollTop(scrollTop + rowTop);
        } else if (rowBottom > scrollHeight) {
            table.scrollTop(scrollTop + (rowBottom - scrollHeight));
        }
    }

    setFilterAll() {
        this.filterLevel("All");
    }

    setFilterDebug() {
        this.filterLevel("Debug");
    }

    setFilterInfo() {
        this.filterLevel("Info");
    }

    setFilterWarning() {
        this.filterLevel("Warn");
    }

    setFilterError() {
        this.filterLevel("Error");
    }

    setFilterFatal() {
        this.filterLevel("Fatal");
    }

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }

    hideLogType(log: logDto) {
        if (!this.filteredLoggers.contains(log.LoggerName)) {
            this.filteredLoggers.push(log.LoggerName);
        }
    }

    unHidelogType(loggerName: string) {
        if (this.filteredLoggers.contains(loggerName)) {
            this.filteredLoggers.remove(loggerName);
        }
    }

    sortBy(columnName, logs, event) {
        if (this.sortColumn() === columnName) {
            this.sortAsc( !this.sortAsc() );
        }
        else {
            this.sortColumn(columnName);
            this.sortAsc(true);
        }
    }
    
    registerColumnResizing() {
        var resizingColumn = false;
        var startX = 0;
        var startingWidth = 0;
        var columnIndex = 0;

        $(document).on("mousedown.logTableColumnResize", ".column-handle", (e: any) => {
            columnIndex = parseInt( $(e.currentTarget).attr("column"));
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
                var targetColumnSize = startingWidth + 100*(e.pageX - startX)/logsRecordsContainerWidth;
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

    unregisterColumnResizing() {
        $(document).off("mousedown.logTableColumnResize");
        $(document).off("mouseup.logTableColumnResize");
        $(document).off("mousemove.logTableColumnResize");
    }

    switchToRegularMode() {
        this.logsMode("Regular");
        this.allLogs.removeAll();
        this.fetchLogs();
        this.disposeHttpTraceClient();
        this.disposeOnDemandLogsClient();
    }

    switchToHttpTraceMode() {
        var tracedDB = appUrl.getResource();
        this.disposeOnDemandLogsClient();
       /* this.logHttpTraceClient= new httpTraceClient(tracedDB.name!=="<system>"?tracedDB:null);
        this.logHttpTraceClient.connectToChangesApiTask
            .done(() => {
                this.logsMode("Http Trace");
                this.allLogs.removeAll();
                this.httpTraceSubscription =
                    this.logHttpTraceClient.watchLogs((e: logNotificationDto) => this.processHttpTraceMessage(e));
            })
            .fail((e) => {
                if (!!e && !!e.status && e.status == 401) {
                    app.showMessage("You do not have the sufficient permissions", "Http-Trace failed to start");
                } else {
                    app.showMessage("Could not open connection", "Http-Trace failed to start");
                }
            });*/
    }

    switchToCustomLogsMode() {
        this.intervalId = setInterval(function () { this.redraw(); }.bind(this), 1000);
        this.allLogs.removeAll();
        this.disposeHttpTraceClient();
        

        var logConfig = new customLogConfig();
        logConfig.maxEntries(10000);
        logConfig.entries.push(new customLogEntry("Raven.", "Info"));
        var customLoggingViewModel = new customLogging(logConfig);
        app.showDialog(customLoggingViewModel);
        customLoggingViewModel.onExit().done((config: customLogConfig) => {
            this.maxEntries(config.maxEntries());
            this.onDemandLogging(new onDemandLogs(this.activeDatabase(), entry => this.onLogMessage(entry)));
            this.onDemandLogging().connectToLogsTask.done(() => {
                this.logsMode("Custom Logger");
                this.customLoggingInProgress(true);
            })
            .fail((e) => {
                if (!!e && !!e.status && e.status == 401) {
                    app.showMessage("You do not have the sufficient permissions", "Custom logging failed to start");
                } else {
                    app.showMessage("Could not open connection", "Custom logging failed to start");
                }
            });

            var categoriesConfig = config.entries().map(e => e.toDto());
            this.onDemandLogging().configureCategories(categoriesConfig);
            this.onDemandLogging().onExit(() => this.customLoggingInProgress(false));
        });
    }

    detached() {
        super.detached();
        this.disposeHttpTraceClient();
        this.disposeOnDemandLogsClient();
    }

    processHttpTraceMessage(e: logNotificationDto) {
        var logObject: logDto;
        logObject = {
            Exception: null,
            Level: e.Level,
            TimeStamp: e.TimeStamp,
            LoggerName: e.LoggerName,
            Message: !e.CustomInfo ? this.formatLogRecord(e) : e.CustomInfo
        };
        this.processLogResults([logObject], true);
    }

    disposeHttpTraceClient() {
//        if (!!this.httpTraceSubscription) {
//            this.httpTraceSubscription.off();
//            this.httpTraceSubscription = null;
//        }
//
//        if (!!this.logHttpTraceClient) {
//            this.logHttpTraceClient.dispose();
//            this.logHttpTraceClient = null;
//        }
    }

    disposeOnDemandLogsClient() {
        var onDemand = this.onDemandLogging();
        if (onDemand) {
            onDemand.dispose();
        }
        this.customLoggingInProgress(false);
        this.onDemandLogging(null);
        this.rawLogs([]);
        this.pendingLogs = [];
        $("#rawLogsContainer pre").empty();
    }


    formatLogRecord(logRecord: logNotificationDto) {
        return 'Request #' + logRecord.RequestId.toString().paddingRight(' ', 4) + ' ' + logRecord.HttpMethod.paddingLeft(' ', 7) + ' - ' + logRecord.EllapsedMiliseconds.toString().paddingRight(' ', 5) + ' ms - ' + logRecord.TenantName.paddingLeft(' ', 10) + ' - ' + logRecord.ResponseStatusCode + ' - ' + logRecord.RequestUri;
    }
    
    onLogMessage(entry: logDto) {
        if (this.rawLogs.length + this.pendingLogs.length < this.maxEntries()) {
            this.pendingLogs.push(entry);
        } else {
            // stop logging
            var onDemand = this.onDemandLogging();
            this.customLoggingInProgress(false);
            onDemand.dispose();
        }
    }

    saveLogs() {
        fileDownloader.downloadAsJson(this.rawLogs(), "logs.json");
    }
}

export = logs;
