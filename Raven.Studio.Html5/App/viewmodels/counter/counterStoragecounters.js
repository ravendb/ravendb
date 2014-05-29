var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "models/counter/counterGroup", "commands/counter/getCountersCommand", "commands/counter/getCounterGroupsCommand", "commands/counter/updateCounterCommand", "viewmodels/viewModelBase", "durandal/app"], function(require, exports, counterGroup, getCountersCommand, getCounterGroupsCommand, updateCounterCommand, viewModelBase, app) {
    var counterStorageCounters = (function (_super) {
        __extends(counterStorageCounters, _super);
        function counterStorageCounters() {
            _super.call(this);
            this.counterGroups = ko.observableArray([]);
            this.selectedCounterGroup = ko.observable();
            this.selectedCountersIndices = ko.observableArray();
            this.currentCountersPagedItems = ko.observable();
            this.fetchGroups();
        }
        counterStorageCounters.prototype.fetchGroups = function () {
            var _this = this;
            new getCounterGroupsCommand(this.activeCounterStorage()).execute().done(function (results) {
                return _this.groupsLoaded(results);
            });
        };
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
        };

        counterStorageCounters.prototype.addOrEditCounter = function (counterToUpdate) {
            var _this = this;
            require(["viewmodels/counter/editCounterDialog"], function (editCounterDialog) {
                var editCounterDialogViewModel = new editCounterDialog(counterToUpdate);
                editCounterDialogViewModel.updateTask.done(function (editedCounter, delta) {
                    new updateCounterCommand(_this.activeCounterStorage(), editedCounter, delta).execute().done(function () {
                        _this.fetchGroups(); //TODO: remove this after changes api is implemented
                    });
                });
                app.showDialog(editCounterDialogViewModel);
            });
        };

        counterStorageCounters.prototype.resetCounter = function (counterToReset) {
            var _this = this;
            app.showMessage('Are you sure you want to reset the counter?', 'Reset Counter', ['Yes', 'No']).done(function (answer) {
                if (answer == "Yes") {
                    require(["commands/counter/resetCounterCommand"], function (resetCounterCommand) {
                        new resetCounterCommand(_this.activeCounterStorage(), counterToReset).execute().done(function () {
                            _this.fetchGroups(); //TODO: remove this after changes api is implemented
                        });
                    });
                }
            });
        };

        counterStorageCounters.prototype.selectGroup = function (group) {
            this.selectedCounterGroup(group);

            if (group.counters().length === 0) {
                var groupName = group.name();
                var command;

                if (groupName == "All Groups") {
                    command = new getCountersCommand(this.activeCounterStorage(), 0, 128);
                } else {
                    command = new getCountersCommand(this.activeCounterStorage(), 0, 128, groupName);
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

            this.selectGroup(groups[0]);
        };
        counterStorageCounters.gridSelector = "#countersGrid";
        return counterStorageCounters;
    })(viewModelBase);

    
    return counterStorageCounters;
});
//# sourceMappingURL=counterStorageCounters.js.map
