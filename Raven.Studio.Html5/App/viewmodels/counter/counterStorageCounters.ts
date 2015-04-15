import counterGroup = require("models/counter/counterGroup");
import counter = require("models/counter/counter");
import getCountersCommand = require("commands/counter/getCountersCommand");
import getCounterGroupsCommand = require("commands/counter/getCounterGroupsCommand");
import updateCounterCommand = require("commands/counter/updateCounterCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import pagedList = require("common/pagedList");
import app = require("durandal/app");
import editCounterDialog = require("viewmodels/counter/editCounterDialog");
import resetCounterCommand = require("commands/counter/resetCounterCommand");

class counterStorageCounters extends viewModelBase {
    counterGroups = ko.observableArray<counterGroup>([]);
    allCounterGroups: counterGroup;
    selectedCounterGroup = ko.observable<counterGroup>();
    selectedCountersIndices = ko.observableArray<number>();
    hasAnyCounterSelected: KnockoutComputed<boolean>;
    currentCountersPagedItems = ko.observable<pagedList>();

    static gridSelector = "#countersGrid";

    constructor() {
        super();
        this.fetchGroups();
    }

    fetchGroups() {
        new getCounterGroupsCommand(this.activeCounterStorage())
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
    }

    addOrEditCounter(counterToUpdate: counter) {
        var editCounterDialogViewModel = new editCounterDialog(counterToUpdate);
        editCounterDialogViewModel.updateTask
            .done((editedCounter: counter, delta: number) => {
                new updateCounterCommand(this.activeCounterStorage(), editedCounter, delta)
                    .execute()
                    .done(() => {
                        this.fetchGroups(); //TODO: remove this after changes api is implemented
                    });
            });
        app.showDialog(editCounterDialogViewModel);
    }

    resetCounter(counterToReset: counter) {
        var confirmationMessageViewModel = this.confirmationMessage('Reset Counter', 'Are you sure you want to reset the counter?');
        confirmationMessageViewModel
            .done(() => {
                new resetCounterCommand(this.activeCounterStorage(), counterToReset)
                    .execute()
                    .done(() => {
                        this.fetchGroups(); //TODO: remove this after changes api is implemented
                    });
            });
    }

    selectGroup(group: counterGroup) {
        this.selectedCounterGroup(group);

        if (group.counters().length === 0) {
            var groupName = group.name();
            var command;

            if (groupName == "All Groups") {
                command = new getCountersCommand(this.activeCounterStorage(), 0, 128);
            } else {
                command = new getCountersCommand(this.activeCounterStorage(), 0, 128, groupName);
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

        this.selectGroup(groups[0]);
    }
}

export = counterStorageCounters;