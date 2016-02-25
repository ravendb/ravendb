import viewModelBase = require("viewmodels/viewModelBase");
import getRequestTracingCommand = require("commands/database/debug/getRequestTracingCommand");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");

class requestTracing extends viewModelBase {
    
    allEntries = ko.observableArray<requestTracingDto>();
    statusFilter = ko.observable("All");
    selectedEntry = ko.observable<requestTracingDto>();

    failedCount: KnockoutComputed<number>;
    successCount: KnockoutComputed<number>;

    constructor() {
        super();

        autoRefreshBindingHandler.install();

        this.failedCount = ko.computed(() => this.allEntries().count(l => l.StatusCode >= 400));
        this.successCount = ko.computed(() => this.allEntries().count(l => l.StatusCode < 400));
        this.activeDatabase.subscribe(() => this.fetchRequestTracing());
    }

    activate(args) {
        super.activate(args);
        return this.fetchRequestTracing();
    }

    fetchRequestTracing(): JQueryPromise<requestTracingDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getRequestTracingCommand(db)
                .execute()
                .done((results: requestTracingDto[]) => this.processResults(results));
        }

        return null;
    }

    processResults(results: requestTracingDto[]) {
        results.forEach(r => {
            r['IsVisible'] = ko.computed(() => this.matchesFilter(r));
        });
        this.allEntries(results.reverse());
    }

    matchesFilter(entry: requestTracingDto) {
        var statusFilter = this.statusFilter();

        var matchesStatusFilter = true;
        if (statusFilter === "Success" && entry.StatusCode >= 400) {
            matchesStatusFilter = false;
        } else if (statusFilter === "Failed" && entry.StatusCode < 400) {
            matchesStatusFilter = false;
        }

        return matchesStatusFilter;
    }

    selectEntry(entry: requestTracingDto) {
        this.selectedEntry(entry);
    }

    tableKeyDown(sender: any, e: KeyboardEvent) {
        
        var isKeyUp = e.keyCode === 38;
        var isKeyDown = e.keyCode === 40;
        if (isKeyUp || isKeyDown) {
            e.preventDefault();

            var oldSelection = this.selectedEntry();
            if (oldSelection) {
                var oldSelectionIndex = this.allEntries.indexOf(oldSelection);
                var newSelectionIndex = oldSelectionIndex;
                if (isKeyUp && oldSelectionIndex > 0) {
                    newSelectionIndex--;
                } else if (isKeyDown && oldSelectionIndex < this.allEntries().length - 1) {
                    newSelectionIndex++;
                }

                this.selectedEntry(this.allEntries()[newSelectionIndex]);
                var newSelectedRow = $("#requestTracingTableContainer table tbody tr:nth-child(" + (newSelectionIndex + 1) + ")");
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
        var table = $("#requestTracingTableContainer");
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

    setStatusAll() {
        this.statusFilter("All");
    }

    setStatusSuccess() {
        this.statusFilter("Success");
    }

    setStatusFailed() {
        this.statusFilter("Failed");
    }
}

export = requestTracing;
