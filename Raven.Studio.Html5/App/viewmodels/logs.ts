import app = require("durandal/app");
import getLogsCommand = require("commands/getLogsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");
import moment = require("moment");
import copyDocuments = require("viewmodels/copyDocuments");
import document = require("models/document");

class logs extends viewModelBase {
    
    allLogs = ko.observableArray<logDto>();
    filterLevel = ko.observable("All");
    selectedLog = ko.observable<logDto>();
    debugLogCount: KnockoutComputed<number>;
    infoLogCount: KnockoutComputed<number>;
    warningLogCount: KnockoutComputed<number>;
    errorLogCount: KnockoutComputed<number>;
    fatalLogCount: KnockoutComputed<number>;
    searchText = ko.observable("");
    searchTextThrottled: KnockoutObservable<string>;
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;
    filteredLoggers = ko.observableArray<string>();

    constructor() {
        super();

        this.debugLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Debug"));
        this.infoLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Info"));
        this.warningLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Warn"));
        this.errorLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Error"));
        this.fatalLogCount = ko.computed(() => this.allLogs().count(l => l.Level === "Fatal"));
        this.searchTextThrottled = this.searchText.throttle(200);
        this.activeDatabase.subscribe(() => this.fetchLogs());
        this.updateCurrentNowTime();
    }

    activate(args) {
        super.activate(args);
        return this.fetchLogs();
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
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

    processLogResults(results: logDto[]) {
        var now = moment();
        results.forEach(r => {
            r['TimeStampText'] = this.createHumanReadableTime(r.TimeStamp);
            r['IsVisible'] = ko.computed(() => this.matchesFilterAndSearch(r) && !this.filteredLoggers.contains(r.LoggerName));
        });
        this.allLogs(results.reverse());
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

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            return ko.computed(() => {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(this.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
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

}

export = logs;