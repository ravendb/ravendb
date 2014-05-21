import counterGroup = require("models/counter/counterGroup");
import counter = require("models/counter/counter");
import getCountersCommand = require("commands/counter/getCountersCommand");
import getCounterGroupsCommand = require("commands/counter/getCounterGroupsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import pagedList = require("common/pagedList");

class counterStorageCounters extends viewModelBase {
    counterGroups = ko.observableArray<counterGroup>([]);
    selectedCounterGroup = ko.observable<counterGroup>();
    currentCountersPagedItems = ko.observable<pagedList>();
    selectedCountersIndices = ko.observableArray<number>();
    hasAnyCounterSelected: KnockoutComputed<boolean>;

    static gridSelector = "#countersGrid";

    constructor() {
        super();

        new getCounterGroupsCommand()
            .execute()
            .done((results: counterGroup[]) => this.groupsLoaded(results));
    }

    getCountersGrid(): virtualTable {
        var gridContents = $(counterStorageCounters.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    // Skip the system database prompt from the base class.
    canActivate(args: any): any {
        return true;
    }

    activate(args) {
        super.activate(args);
        this.hasAnyCounterSelected = ko.computed(() => this.selectedCountersIndices().length > 0);

        //this.loadCounters(false);
    }

    selectGroup(group: counterGroup) {
        this.selectedCounterGroup(group);

        if (group.counters().length === 0) {
            new getCountersCommand(0, 128, group.name())
                .execute()
                .done((results: counter[]) => group.counters(results));
        }
    }

    groupsLoaded(groups: counterGroup[]) {
        this.counterGroups(groups);
        if (groups.length) {
            this.selectedCounterGroup(groups[0]);
        }
    }
}

export = counterStorageCounters;