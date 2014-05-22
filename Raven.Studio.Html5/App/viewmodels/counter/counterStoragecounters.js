var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "models/counter/counterGroup", "commands/counter/getCountersCommand", "commands/counter/getCounterGroupsCommand", "viewmodels/viewModelBase"], function(require, exports, counterGroup, getCountersCommand, getCounterGroupsCommand, viewModelBase) {
    var counterStorageCounters = (function (_super) {
        __extends(counterStorageCounters, _super);
        function counterStorageCounters() {
            var _this = this;
            _super.call(this);
            this.counterGroups = ko.observableArray([]);
            this.selectedCounterGroup = ko.observable();
            this.currentCountersPagedItems = ko.observable();
            this.selectedCountersIndices = ko.observableArray();

            new getCounterGroupsCommand().execute().done(function (results) {
                return _this.groupsLoaded(results);
            });
        }
        counterStorageCounters.prototype.getCountersGrid = function () {
            var gridContents = $(counterStorageCounters.gridSelector).children()[0];
            if (gridContents) {
                return ko.dataFor(gridContents);
            }

            return null;
        };

        // Skip the system database prompt from the base class.
        counterStorageCounters.prototype.canActivate = function (args) {
            return true;
        };

        counterStorageCounters.prototype.activate = function (args) {
            var _this = this;
            _super.prototype.activate.call(this, args);
            this.hasAnyCounterSelected = ko.computed(function () {
                return _this.selectedCountersIndices().length > 0;
            });
            //this.loadCounters(false);
        };

        counterStorageCounters.prototype.selectGroup = function (group) {
            this.selectedCounterGroup(group);

            if (group.counters().length === 0) {
                var groupName = group.name();
                var command;

                if (groupName == "All Groups") {
                    command = new getCountersCommand(0, 128);
                } else {
                    command = new getCountersCommand(0, 128, groupName);
                }
                command.execute().done(function (results) {
                    return group.counters(results);
                });
            }
        };

        counterStorageCounters.prototype.groupsLoaded = function (groups) {
            var _this = this;
            this.counterGroups(groups);

            this.allCounterGroups = new counterGroup({ Name: "All Groups" }); // Create the "All Groups" pseudo collection.
            this.allCounterGroups.numOfCounters = ko.computed(function () {
                return _this.counterGroups().filter(function (c) {
                    return c !== _this.allCounterGroups;
                }).map(function (c) {
                    return c.numOfCounters();
                }).reduce(function (first, second) {
                    return first + second;
                }, 0);
            }); // And sum them up.

            this.counterGroups.unshift(this.allCounterGroups);

            this.selectedCounterGroup(groups[0]);
        };
        counterStorageCounters.gridSelector = "#countersGrid";
        return counterStorageCounters;
    })(viewModelBase);

    
    return counterStorageCounters;
});
//# sourceMappingURL=counterStorageCounters.js.map
