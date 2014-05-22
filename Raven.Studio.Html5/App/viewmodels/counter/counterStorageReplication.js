var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "common/appUrl", "models/filesystem/synchronizationReplicationSetup", "models/filesystem/synchronizationDestination", "viewmodels/viewModelBase", "commands/filesystem/getDestinationsCommand", "commands/filesystem/getFilesystemStatsCommand", "commands/filesystem/saveDestinationCommand"], function(require, exports, appUrl, synchronizationReplicationSetup, synchronizationDestination, viewModelBase, getDestinationsCommand, getFilesystemStatsCommand, saveDestinationCommand) {
    var counterStorageReplication = (function (_super) {
        __extends(counterStorageReplication, _super);
        function counterStorageReplication() {
            _super.apply(this, arguments);
            this.dirtyFlag = new ko.DirtyFlag([]);
            this.replicationsSetup = ko.observable(new synchronizationReplicationSetup({ Destinations: [], Source: null }));
        }
        counterStorageReplication.prototype.canActivate = function (args) {
            var _this = this;
            var deferred = $.Deferred();
            var fs = this.activeFilesystem();
            if (fs) {
                this.fetchDestinations().done(function () {
                    return deferred.resolve({ can: true });
                }).fail(function () {
                    return deferred.resolve({ redirect: appUrl.forFilesystem(_this.activeFilesystem()) });
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

        counterStorageReplication.prototype.deactivate = function () {
            _super.prototype.deactivate.call(this);
        };

        counterStorageReplication.prototype.saveChanges = function () {
            var _this = this;
            if (this.replicationsSetup().source()) {
                this.saveReplicationSetup();
            } else {
                var fs = this.activeFilesystem();
                if (fs) {
                    new getFilesystemStatsCommand(fs).execute().done(function (result) {
                        _this.prepareAndSaveReplicationSetup(result.DatabaseId);
                    });
                }
            }
        };

        counterStorageReplication.prototype.prepareAndSaveReplicationSetup = function (source) {
            this.replicationsSetup().source(source);
            this.saveReplicationSetup();
        };

        counterStorageReplication.prototype.saveReplicationSetup = function () {
            var _this = this;
            var fs = this.activeFilesystem();
            if (fs) {
                var self = this;
                new saveDestinationCommand(this.replicationsSetup().toDto(), fs).execute().done(function () {
                    return _this.dirtyFlag().reset();
                });
            }
        };

        counterStorageReplication.prototype.createNewDestination = function () {
            this.replicationsSetup().destinations.unshift(synchronizationDestination.empty());
        };

        counterStorageReplication.prototype.removeDestination = function (repl) {
            this.replicationsSetup().destinations.remove(repl);
        };

        counterStorageReplication.prototype.fetchDestinations = function () {
            var _this = this;
            var deferred = $.Deferred();
            var fs = this.activeFilesystem();
            if (fs) {
                new getDestinationsCommand(fs).execute().done(function (data) {
                    return _this.replicationsSetup(new synchronizationReplicationSetup({ Destinations: data.Destinations, Source: null }));
                }).always(function () {
                    return deferred.resolve({ can: true });
                });
            }
            return deferred;
        };
        return counterStorageReplication;
    })(viewModelBase);

    
    return counterStorageReplication;
});
//# sourceMappingURL=counterStorageReplication.js.map
