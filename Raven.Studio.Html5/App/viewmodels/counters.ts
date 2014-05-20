import counterGroup = require("models/counter/counterGroup");
import counter = require("models/counter/counter");
import getCountersCommand = require("commands/counter/getCountersCommand");
import getCounterGroupsCommand = require("commands/counter/getCounterGroupsCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class counters extends viewModelBase {
    counterGroups = ko.observableArray<counterGroup>([]);
    selectedCounterGroup = ko.observable<counterGroup>();

    constructor() {
        super();

        new getCounterGroupsCommand()
            .execute()
            .done((results: counterGroup[]) => this.groupsLoaded(results));
    }

    // Skip the system database prompt from the base class.
    canActivate() {
        return true;
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

export = counters; 