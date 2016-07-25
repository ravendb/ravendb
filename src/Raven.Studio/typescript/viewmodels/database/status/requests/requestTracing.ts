import viewModelBase = require("viewmodels/viewModelBase");
import getRequestTracingCommand = require("commands/database/debug/getRequestTracingCommand");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");
import tableNavigationTrait = require("common/tableNavigationTrait");

class requestTracing extends viewModelBase {
    
    allEntries = ko.observableArray<requestTracingDto>();
    statusFilter = ko.observable("All");
    selectedEntry = ko.observable<requestTracingDto>();

    failedCount: KnockoutComputed<number>;
    successCount: KnockoutComputed<number>;

    tableNavigation: tableNavigationTrait<requestTracingDto>;

    constructor() {
        super();

        autoRefreshBindingHandler.install();

        this.failedCount = ko.computed(() => this.allEntries().count(l => l.StatusCode >= 400));
        this.successCount = ko.computed(() => this.allEntries().count(l => l.StatusCode < 400));
        this.activeDatabase.subscribe(() => this.fetchRequestTracing());

        this.tableNavigation = new tableNavigationTrait<requestTracingDto>("#requestTracingTableContainer", this.selectedEntry, this.allEntries, i => "#requestTracingTableContainer table tbody tr:nth-child(" + (i + 1) + ")");
    }

    activate(args: any) {
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
        results.forEach((r: any) => {
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

    showContextMenu() {
        //alert("this");
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
