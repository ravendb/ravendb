var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/counter/getCountersCommand", "commands/counter/getCounterGroupsCommand", "viewmodels/viewModelBase"], function(require, exports, getCountersCommand, getCounterGroupsCommand, viewModelBase) {
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
                new getCountersCommand(0, 128, group.name()).execute().done(function (results) {
                    return group.counters(results);
                });
            }
        };

        counterStorageCounters.prototype.groupsLoaded = function (groups) {
            this.counterGroups(groups);
            if (groups.length) {
                this.selectedCounterGroup(groups[0]);
            }
        };
        counterStorageCounters.gridSelector = "#countersGrid";
        return counterStorageCounters;
    })(viewModelBase);

    
    return counterStorageCounters;
});
//# sourceMappingURL=counterStorageCounters.js.map
