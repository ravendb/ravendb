var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "durandal/app", "common/appUrl", "viewmodels/viewModelBase", "models/counter/counterStorageReplicationSetup", "models/counter/counterStorageReplicationDestination", "commands/counter/getCounterStorageReplicationCommand", "commands/counter/saveCounterStorageReplicationCommand"], function(require, exports, app, appUrl, viewModelBase, counterStorageReplicationSetup, counterStorageReplicationDestination, getCounterStorageReplicationCommand, saveCounterStorageReplicationCommand) {
    var counterStorageReplication = (function (_super) {
        __extends(counterStorageReplication, _super);
        function counterStorageReplication() {
            _super.apply(this, arguments);
            this.replicationsSetup = ko.observable().extend({ required: true });
        }
        counterStorageReplication.prototype.canActivate = function (args) {
            var deferred = $.Deferred();
            var counterStorage = this.activeCounterStorage();
            if (counterStorage) {
                this.fetchCountersDestinations(counterStorage).done(function () {
                    return deferred.resolve({ can: true });
                }).fail(function () {
                    return deferred.resolve({ redirect: appUrl.forCounterStorage(counterStorage) });
                });
            }
            return deferred;
        };

        counterStorageReplication.prototype.activate = function (args) {
            _super.prototype.activate.call(this, args);

            viewModelBase.dirtyFlag = new ko.DirtyFlag([this.replicationsSetup]);
            this.isSaveEnabled = ko.computed(function () {
                return viewModelBase.dirtyFlag().isDirty();
            });
        };

        counterStorageReplication.prototype.fetchCountersDestinations = function (counterStorage, reportFetchProgress) {
            var _this = this;
            if (typeof reportFetchProgress === "undefined") { reportFetchProgress = false; }
            var deferred = $.Deferred();
            if (counterStorage) {
                new getCounterStorageReplicationCommand(counterStorage, reportFetchProgress).execute().done(function (data) {
                    return _this.replicationsSetup(new counterStorageReplicationSetup({ Destinations: data.Destinations }));
                }).fail(function () {
                    return _this.replicationsSetup(new counterStorageReplicationSetup({ Destinations: [] }));
                }).always(function () {
                    return deferred.resolve({ can: true });
                });
            }
            return deferred;
        };

        counterStorageReplication.prototype.saveChanges = function () {
            var counter = this.activeCounterStorage();
            if (counter) {
                new saveCounterStorageReplicationCommand(this.replicationsSetup().toDto(), counter).execute().done(function () {
                    return viewModelBase.dirtyFlag().reset();
                });
            }
        };

        counterStorageReplication.prototype.createNewDestination = function () {
            this.replicationsSetup().destinations.unshift(counterStorageReplicationDestination.empty());
        };

        counterStorageReplication.prototype.removeDestination = function (resplicationDestination) {
            this.replicationsSetup().destinations.remove(resplicationDestination);
        };

        counterStorageReplication.prototype.refreshFromServer = function () {
            var _this = this;
            this.showRefreshPrompt().done(function (answer) {
                if (answer == "Yes") {
                    _this.fetchCountersDestinations(_this.activeCounterStorage(), true).done(function () {
                        return viewModelBase.dirtyFlag().reset();
                    });
                }
            });
        };

        counterStorageReplication.prototype.showRefreshPrompt = function () {
            var deferred = $.Deferred();
            var isDirty = viewModelBase.dirtyFlag().isDirty();
            if (isDirty) {
                return app.showMessage('You have unsaved data. Are you sure you want to refresh the data from the server?', 'Unsaved Data', ['Yes', 'No']);
            }
            return deferred.resolve("Yes");
        };
        return counterStorageReplication;
    })(viewModelBase);

    
    return counterStorageReplication;
});
//# sourceMappingURL=counterStorageReplication.js.map
