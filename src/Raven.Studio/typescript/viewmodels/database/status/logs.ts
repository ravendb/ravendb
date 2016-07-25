import getLogsCommand = require("commands/database/debug/getLogsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import moment = require("moment");
import document = require("models/database/documents/document");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");
import logEntry = require("models/database/debug/logEntry");
import tableNavigationTrait = require("common/tableNavigationTrait");

class logs extends viewModelBase {

    allLogs = ko.observableArray<logEntry>();
    filterLevel = ko.observable("All");
    selectedLog = ko.observable<logEntry>();
    debugLogCount: KnockoutComputed<number>;
    infoLogCount: KnockoutComputed<number>;
    warningLogCount: KnockoutComputed<number>;
    errorLogCount: KnockoutComputed<number>;
    fatalLogCount: KnockoutComputed<number>;
    searchText = ko.observable("");
    searchTextThrottled: KnockoutObservable<string>;
    now = ko.observable<moment.Moment>();
    updateNowTimeoutHandle = 0;
    filteredLoggers = ko.observableArray<string>();
    sortColumn = ko.observable<string>("timeStamp");
    sortAsc = ko.observable<boolean>(true);
    filteredAndSortedLogs: KnockoutComputed<Array<logDto>>;
    columnWidths: Array<KnockoutObservable<number>>;
    showLogDetails = ko.observable<boolean>(false);

    tableNavigation: tableNavigationTrait<logEntry>;

    constructor() {
        super();

        autoRefreshBindingHandler.install();

        this.debugLogCount = ko.pureComputed(() => this.allLogs().count(l => l.level() === "Debug"));
        this.infoLogCount = ko.pureComputed(() => this.allLogs().count(l => l.level() === "Info"));
        this.warningLogCount = ko.pureComputed(() => this.allLogs().count(l => l.level() === "Warn"));
        this.errorLogCount = ko.pureComputed(() => this.allLogs().count(l => l.level() === "Error"));
        this.fatalLogCount = ko.pureComputed(() => this.allLogs().count(l => l.level() === "Fatal"));
        this.searchTextThrottled = this.searchText.throttle(400);
        this.activeDatabase.subscribe(() => this.fetchLogs());
        this.updateCurrentNowTime();

        this.sortColumn.subscribe(() => this.sortInPlace());
        this.sortAsc.subscribe(() => this.sortInPlace());

        this.tableNavigation = new tableNavigationTrait<logEntry>("#logRecords", this.selectedLog, this.allLogs, (i: number) => "#logRecords > div:nth-child(" + (i + 1) + ")");
    }

    activate(args: any) {
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
        this.showLogDetails.subscribe(() => {
                $(".logRecords").toggleClass("logRecords-small");
        });

        $("#logRecordsContainer").width();
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
        var mappedResults: logEntry[] = results.map((r: logDto) => {
            var mapped: logEntry = new logEntry(r, this.now);
            mapped.isVisible = ko.pureComputed(() => {
                var matchesSearch = this.matchesFilterAndSearch(mapped);
                var matchesFilters = this.filteredLoggers().contains(mapped.loggerName());
                return matchesSearch && !matchesFilters;
            });
            return mapped;
        });

        var sortedResults = this.sortResults(mappedResults.reverse());
        var allLogsRaw = this.allLogs();
        if (sortedResults.length === allLogsRaw.length) {
            for (var i = 0; i < sortedResults.length; i++) {
                allLogsRaw[i].copyFrom(sortedResults[i]);
            }
        } else {
            this.allLogs(sortedResults);
        }
    }

    sortInPlace() {
        var dataToSort = this.sortResults(this.allLogs());
        this.allLogs(dataToSort);
    }

    sortResults(dataToSort: logEntry[]) {
        var column = this.sortColumn();
        var asc = this.sortAsc();
        var test = asc ? ((l: any, r:  any) => l < r) : ((l: any, r: any) => l > r);

        var sortFunc = (left: any, right: any) => {
            if (left[column]() === right[column]()) { return 0; }
            return test(left[column](), right[column]()) ? 1 : -1;
        }

        return dataToSort.sort(sortFunc);
    }

    matchesFilterAndSearch(log: logEntry) {
        var searchTextThrottled = this.searchTextThrottled().toLowerCase();
        var filterLevel = this.filterLevel();
        var matchesLogLevel = filterLevel === "All" || log.level() === filterLevel;
        var matchesSearchText = !searchTextThrottled ||
            (log.message() && log.message().toLowerCase().indexOf(searchTextThrottled) >= 0) ||
            (log.exception() && log.exception().toLowerCase().indexOf(searchTextThrottled) >= 0);

        return matchesLogLevel && matchesSearchText;
    }

    selectLog(log: logEntry) {
        this.selectedLog(log);
        this.showLogDetails(true);
        $(".logRecords").addClass("logRecords-small");
    }

    unSelectLog(log: logEntry) {
        this.selectedLog(null);
        this.showLogDetails(false);
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

    hideLogType(log: logEntry) {
        if (!this.filteredLoggers.contains(log.loggerName())) {
            this.filteredLoggers.push(log.loggerName());
        }
    }

    unHidelogType(loggerName: string) {
        if (this.filteredLoggers.contains(loggerName)) {
            this.filteredLoggers.remove(loggerName);
        }
    }

    sortBy(columnName: string) {
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
