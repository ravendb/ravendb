import getLogsCommand = require("commands/database/debug/getLogsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import document = require("models/database/documents/document");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");

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
    sortColumn = ko.observable<string>("TimeStamp");
    sortAsc = ko.observable<boolean>(true);
    filteredAndSortedLogs: KnockoutComputed<Array<logDto>>;
    columnWidths: Array<KnockoutObservable<number>>;
    showLogDetails = ko.observable<boolean>(false);

    constructor() {
        super();

        autoRefreshBindingHandler.install();

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
        this.updateHelpLink('3Z9IJS');
        return this.fetchLogs();
    }

    attached() {
        super.attached();
        this.showLogDetails.subscribe(x => {
                $(".logRecords").toggleClass("logRecords-small");
        });

        var logsRecordsContainerWidth = $("#logRecordsContainer").width();
        var widthUnit = 0.08;
        this.columnWidths[0](100 * widthUnit);
        this.columnWidths[1](100 * widthUnit);
        this.columnWidths[2](100 * widthUnit * 6);
        this.columnWidths[3](100 * widthUnit * 2);
        this.columnWidths[4](100 * widthUnit * 2);
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
        this.unregisterColumnResizing();
    }

    fetchLogs(): JQueryPromise<logDto[]> {
        var db = this.activeDatabase();
        if (db) {
            var deferred = $.Deferred();
            new getLogsCommand(db)
                .execute()
                .done((results: logDto[]) => {
                    this.processLogResults(results);
                    deferred.resolve(results);
                });
            return deferred;
        }

        return null;
    }

    processLogResults(results: logDto[]) {
        results.forEach(r => {
            r['HumanizedTimestamp'] = this.createHumanReadableTime(r.TimeStamp,true,false);
            r['TimeStampText'] = this.createHumanReadableTime(r.TimeStamp,true,true);
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

    createHumanReadableTime(time: string, chainHumanized: boolean= true, chainDateTime:boolean=true): KnockoutComputed<string> {
        if (time) {
            return ko.computed(() => {
                var dateMoment = moment(time);
                var humanized = "", formattedDateTime = "";
                var agoInMs = dateMoment.diff(this.now());
                if (chainHumanized)
                    humanized = moment.duration(agoInMs).humanize(true);
                if (chainDateTime)
                    formattedDateTime = dateMoment.format(" (MM/DD/YY, h:mma)");
                return humanized + formattedDateTime;
            });
        }

        return ko.computed(() => time);
    }

    selectLog(log: logDto) {
        this.selectedLog(log);
        this.showLogDetails(true);
        $(".logRecords").addClass("logRecords-small");
    }

    unSelectLog(log: logDto) {
        this.selectedLog(null);
        this.showLogDetails(false);
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
}

export = logs;
