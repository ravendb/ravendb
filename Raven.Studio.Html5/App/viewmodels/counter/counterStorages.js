var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "viewmodels/viewModelBase", "commands/counter/getCounterStoragesCommand", "common/appUrl"], function(require, exports, viewModelBase, getCounterStoragesCommand, appUrl) {
    var counterStorages = (function (_super) {
        __extends(counterStorages, _super);
        function counterStorages() {
            _super.apply(this, arguments);
            var _this = this;
            this.storages = ko.observableArray();
            this.hasCounterStorages = ko.computed(function () {
                return _this.storages().length > 0;
            });
            this.isFirstLoad = true;
            this.selectedCounterStorage = ko.observable();
        }
        counterStorages.prototype.canActivate = function (args) {
            return true;
        };

        counterStorages.prototype.createNewCountersStorage = function () {
            //todo: implement creation of new counter storage
        };

        counterStorages.prototype.countersStoragesLoaded = function (storages) {
            if (storages.length === 0 && this.isFirstLoad) {
                this.createNewCountersStorage();
            }

            var counterSotragesHaveChanged = this.checkDifferentCounterStorages(storages);
            if (counterSotragesHaveChanged) {
                this.storages(storages);
            }

            this.isFirstLoad = false;
        };

        counterStorages.prototype.checkDifferentCounterStorages = function (storages) {
            if (storages.length !== this.storages().length) {
                return true;
            }

            var freshStorageNames = storages.map(function (storage) {
                return storage.name;
            });
            var existingStorageNames = this.storages().map(function (d) {
                return d.name;
            });
            return existingStorageNames.some(function (existing) {
                return !freshStorageNames.contains(existing);
            });
        };

        counterStorages.prototype.modelPolling = function () {
            var _this = this;
            new getCounterStoragesCommand().execute().done(function (results) {
                return _this.countersStoragesLoaded(results);
            });
        };

        counterStorages.prototype.selectCounterStorage = function (storage) {
            this.storages().forEach(function (d) {
                return d.isSelected(d == storage);
            });
            storage.activate();
            this.selectedCounterStorage(storage);
        };

        counterStorages.prototype.getCounterStorageUrl = function (storage) {
            return appUrl.forCounterStorageCounters(storage);
        };
        return counterStorages;
    })(viewModelBase);

    
    return counterStorages;
});
//# sourceMappingURL=counterStorages.js.map
