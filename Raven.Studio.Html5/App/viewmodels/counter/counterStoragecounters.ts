import counterGroup = require("models/counter/counterGroup");
import counter = require("models/counter/counter");
import getCountersCommand = require("commands/counter/getCountersCommand");
import getCounterGroupsCommand = require("commands/counter/getCounterGroupsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import pagedList = require("common/pagedList");

class counterStorageCounters extends viewModelBase {
    counterGroups = ko.observableArray<counterGroup>([]);
    allCounterGroups: counterGroup;
    selectedCounterGroup = ko.observable<counterGroup>();
    currentCountersPagedItems = ko.observable<pagedList>();
    selectedCountersIndices = ko.observableArray<number>();
    hasAnyCounterSelected: KnockoutComputed<boolean>;

    static gridSelector = "#countersGrid";

    constructor() {
        super();

        new getCounterGroupsCommand()
            .execute()
            .done((results: counterGroup[])=> this.groupsLoaded(results));
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
            var groupName = group.name();
            var command;

            if (groupName == "All Groups") {
                command = new getCountersCommand(0, 128);
            } else {
                command = new getCountersCommand(0, 128, groupName);
            }
            command
                .execute()
                .done((results: counter[]) => group.counters(results));
        }
    }

    groupsLoaded(groups: counterGroup[]) {
        this.counterGroups(groups);

        this.allCounterGroups = new counterGroup({ Name: "All Groups" }); // Create the "All Groups" pseudo collection.
        this.allCounterGroups.numOfCounters = ko.computed(() =>
            this.counterGroups()
                .filter(c => c !== this.allCounterGroups) // Don't include self, the all counter groups.
                .map(c => c.numOfCounters()) // Grab the counter count of each.
                .reduce((first: number, second: number) => first + second, 0)); // And sum them up.

        this.counterGroups.unshift(this.allCounterGroups);

        this.selectedCounterGroup(groups[0]);
    }
}

export = counterStorageCounters;