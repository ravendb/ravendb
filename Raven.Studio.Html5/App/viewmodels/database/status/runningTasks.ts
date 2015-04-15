import viewModelBase = require("viewmodels/viewModelBase");
import killRunningTaskCommand = require("commands/operations/killRunningTaskCommand");
import getRunningTasksCommand = require("commands/operations/getRunningTasksCommand");
import moment = require("moment");
import document = require("models/database/documents/document");

class runningTasks extends viewModelBase {

    static TypeSuggestionQuery = "SuggestionQuery";
    static TypeBulkInsert = "BulkInsert";
    static TypeIndexBulkOperation = "IndexBulkOperation";
    static TypeIndexDeleteOperation = "IndexDeleteOperation";
    static TypeImportDatabase = "ImportDatabase";
    static TypeRestoreDatabase = "RestoreDatabase";
    static TypeRestoreFilesystem = "RestoreFilesystem";
    static TypeCompactDatabase = "CompactDatabase";
    static TypeCompactFilesystem = "CompactFilesystem";
	static TypeIoTest = "IoTest";
	static TypeNewIndexPrecomputedBatch = "NewIndexPrecomputedBatch";
    
    allTasks = ko.observableArray<runningTaskDto>();
    filterType = ko.observable<string>(null);
    selectedTask = ko.observable<runningTaskDto>();
    noExceptionText = ko.computed(() => !!this.selectedTask() && !!this.selectedTask().ExceptionText);

    suggestionQueryCount: KnockoutComputed<number>;
    bulkInsertCount: KnockoutComputed<number>;
    indexBulkOperationCount: KnockoutComputed<number>;
    indexDeleteOperationCount: KnockoutComputed<number>;
    importDatabaseCount: KnockoutComputed<number>;
    restoreDatabaseCount: KnockoutComputed<number>;
    restoreFilesystemCount: KnockoutComputed<number>;
    compactDatabaseCount: KnockoutComputed<number>;
    compactFilesystemCount: KnockoutComputed<number>;
	ioTestCount: KnockoutComputed<number>;
	newIndexPrecomputedBatchCount: KnockoutComputed<number>;

    searchText = ko.observable("");
    searchTextThrottled: KnockoutObservable<string>;
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;
    sortColumn = ko.observable<string>("logged");
    sortAsc = ko.observable<boolean>(true);
    filteredAndSortedTasks: KnockoutComputed<Array<runningTaskDto>>;
    columnWidths: Array<KnockoutObservable<number>>;

    constructor() {
        super();

        this.suggestionQueryCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeSuggestionQuery));
        this.bulkInsertCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeBulkInsert));
        this.indexBulkOperationCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeIndexBulkOperation));
        this.indexDeleteOperationCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeIndexDeleteOperation));
        this.importDatabaseCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeImportDatabase));
        this.restoreDatabaseCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeRestoreDatabase));
        this.restoreFilesystemCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeRestoreFilesystem));
        this.compactDatabaseCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeCompactDatabase));
        this.compactFilesystemCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeCompactFilesystem));
		this.ioTestCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeIoTest));
		this.newIndexPrecomputedBatchCount = ko.computed(() => this.allTasks().count(l => l.TaskType === runningTasks.TypeNewIndexPrecomputedBatch));

        this.searchTextThrottled = this.searchText.throttle(200);
        this.activeDatabase.subscribe(() => this.fetchTasks());
        this.updateCurrentNowTime();

        this.filteredAndSortedTasks = ko.computed<Array<runningTaskDto>>(() => {
            var tasks = this.allTasks();
            var column = this.sortColumn();
            var asc = this.sortAsc();

            var sortFunc = (left, right) => {
                if (left[column] === right[column]) { return 0; }
                var test = asc ? ((l, r) => l < r) : ((l, r) => l > r);
                return test(left[column], right[column]) ? 1 : -1;
            }

            return tasks.sort(sortFunc);
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
        this.updateHelpLink('2KH22A');
        return this.fetchTasks();
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
        this.unregisterColumnResizing();
    }

    fetchTasks(): JQueryPromise<runningTaskDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getRunningTasksCommand(db)
                .execute()
                .done((results: runningTaskDto[]) => this.processRunningTasksResults(results));
        }

        return null;
    }

    processRunningTasksResults(results: runningTaskDto[]) {
        var now = moment();
        results.forEach(r => {
            r['TimeStampText'] = this.createHumanReadableTime(r.StartTime);
            r['IsVisible'] = ko.computed(() => this.matchesFilterAndSearch(r));
            r['Killable'] = ko.computed(() => this.canKill(r));
            r['ExceptionText'] = r.Exception ? JSON.stringify(r.Exception, null, 2) : '';
        });
        this.allTasks(results.reverse());
    }

    canKill(task: runningTaskDto) { 
        var status = task.TaskStatus === "Running"
            || task.TaskStatus === "Created"
            || task.TaskStatus === "WaitingToRun"
            || task.TaskStatus === "WaitingForActivation";
        var taskType = task.TaskType != runningTasks.TypeIndexDeleteOperation
            && task.TaskType != runningTasks.TypeSuggestionQuery
            && task.TaskType != runningTasks.TypeRestoreDatabase
            && task.TaskType != runningTasks.TypeRestoreFilesystem
            && task.TaskType != runningTasks.TypeCompactDatabase
            && task.TaskType != runningTasks.TypeCompactFilesystem;
        return status && taskType;
    }

    matchesFilterAndSearch(task: runningTaskDto) {
        var searchTextThrottled = this.searchTextThrottled().toLowerCase();
        var filterType = this.filterType();
        var matchesLogLevel = filterType === null || task.TaskType === filterType;
        var matchesSearchText = !searchTextThrottled ||
            (task.Payload && task.Payload.toLowerCase().indexOf(searchTextThrottled) >= 0) ||
            (task.ExceptionText && task.ExceptionText.toLowerCase().indexOf(searchTextThrottled) >= 0);

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

    selectTask(task: runningTaskDto) {
        this.selectedTask(task);
    }

    taskKill(task: runningTaskDto) {
        new killRunningTaskCommand(this.activeDatabase(), task.Id).execute()
            .always(() => setTimeout(() => {
                this.selectedTask(null);
                this.fetchTasks();
            }, 1000));
    }

    tableKeyDown(sender: any, e: KeyboardEvent) {
        var isKeyUp = e.keyCode === 38;
        var isKeyDown = e.keyCode === 40;
        if (isKeyUp || isKeyDown) {
            e.preventDefault();

            var oldSelection = this.selectedTask();
            if (oldSelection) {
                var oldSelectionIndex = this.allTasks.indexOf(oldSelection);
                var newSelectionIndex = oldSelectionIndex;
                if (isKeyUp && oldSelectionIndex > 0) {
                    newSelectionIndex--;
                } else if (isKeyDown && oldSelectionIndex < this.allTasks().length - 1) {
                    newSelectionIndex++;
                }

                this.selectedTask(this.allTasks()[newSelectionIndex]);
                var newSelectedRow = $("#runningTasksContainer table tbody tr:nth-child(" + (newSelectionIndex + 1) + ")");
                if (newSelectedRow) {
                    this.ensureRowVisible(newSelectedRow);
                }
            }
        }
    }

    ensureRowVisible(row: JQuery) {
        var table = $("#runningTasksTableContainer");
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

    setFilterTypeAll() {
        this.filterType(null);
    }

    setFilterTypeSuggestionQuery() {
        this.filterType(runningTasks.TypeSuggestionQuery);
    }

    setFilterTypeBulkInsert() {
        this.filterType(runningTasks.TypeBulkInsert);
    }

    setFilterTypeIndexBulkOperation() {
        this.filterType(runningTasks.TypeIndexBulkOperation);
    }

    setFilterTypeIndexDeleteOperation() {
        this.filterType(runningTasks.TypeIndexDeleteOperation);
    }

    setFilterTypeImportDatabase() {
        this.filterType(runningTasks.TypeImportDatabase);
    }

    setFilterTypeRestoreDatabase() {
        this.filterType(runningTasks.TypeRestoreDatabase);
    }

    setFilterTypeRestoreFilesystem() {
        this.filterType(runningTasks.TypeRestoreFilesystem);
    }

    setFilterTypeCompactDatabase() {
        this.filterType(runningTasks.TypeCompactDatabase);
    }

    setFilterTypeCompactFilesystem() {
        this.filterType(runningTasks.TypeCompactFilesystem);
    }

    setFilterTypeIoTest() {
        this.filterType(runningTasks.TypeIoTest);
	}

	setFilterTypeNewIndexPrecomputedBatch() {
		this.filterType(runningTasks.TypeNewIndexPrecomputedBatch);
	}

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }

    sortBy(columnName, logs, event) {
        if (this.sortColumn() === columnName) {
            this.sortAsc(!this.sortAsc());
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

        $(document).on("mousedown.tasksTableColumnResize", ".column-handle", (e: any) => {
            columnIndex = parseInt($(e.currentTarget).attr("column"));
            startingWidth = this.columnWidths[columnIndex]();
            startX = e.pageX;
            resizingColumn = true;
        });

        $(document).on("mouseup.tasksTableColumnResize", "", (e: any) => {
            resizingColumn = false;
        });

        $(document).on("mousemove.tasksTableColumnResize", "", (e: any) => {
            if (resizingColumn) {
                var targetColumnSize = startingWidth + e.pageX - startX;
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
        $(document).off("mousedown.tasksTableColumnResize");
        $(document).off("mouseup.tasksTableColumnResize");
        $(document).off("mousemove.tasksTableColumnResize");
    }
}

export = runningTasks;
