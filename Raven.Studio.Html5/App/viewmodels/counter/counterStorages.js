var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "durandal/app", "viewmodels/viewModelBase", "models/counter/counterStorage", "commands/counter/getCounterStoragesCommand", "commands/counter/createCounterStorageCommand", "common/appUrl"], function(require, exports, app, viewModelBase, counterStorage, getCounterStoragesCommand, createCounterStorageCommand, appUrl) {
    var counterStorages = (function (_super) {
        __extends(counterStorages, _super);
        function counterStorages() {
            var _this = this;
            _super.call(this);
            this.storages = ko.observableArray();
            this.hasCounterStorages = ko.computed(function () {
                return _this.storages().length > 0;
            });
            this.isFirstLoad = true;
            this.selectedCounterStorage = ko.observable();
            this.searchCounterStorageByText = ko.observable();
            this.searchCounterStorageByText.extend({ throttle: 200 }).subscribe(function (s) {
                return _this.filterCounterStorages(s);
            });
        }
        counterStorages.prototype.canActivate = function (args) {
            return true;
        };

        counterStorages.prototype.filterCounterStorages = function (filterString) {
            var filterStringLower = filterString.toLowerCase();
            this.storages().forEach(function (x) {
                var isMatch = !filterString || (x.name.toLowerCase().indexOf(filterStringLower) >= 0);
                x.isVisible(isMatch);
            });

            var selectedCounterStorage = this.selectedCounterStorage();
            if (selectedCounterStorage && !selectedCounterStorage.isVisible()) {
                selectedCounterStorage.isSelected(false);
                this.selectedCounterStorage(null);
            }
        };

        counterStorages.prototype.deleteSelectedCounterStorage = function () {
            var _this = this;
            var counterStorage = this.selectedCounterStorage();
            if (!!counterStorage) {
                require(["viewmodels/counter/deleteCounterStorageConfirm"], function (deleteCounterStorageConfirm) {
                    var confirmDeleteVm = new deleteCounterStorageConfirm(counterStorage);
                    confirmDeleteVm.deleteTask.done(function () {
                        _this.onCounterStorageDeleted(counterStorage);
                        _this.selectedCounterStorage(null);
                    });
                    app.showDialog(confirmDeleteVm);
                });
            }
        };

        counterStorages.prototype.onCounterStorageDeleted = function (storage) {
            this.storages.remove(storage);
            if (this.storages.length > 0 && this.storages.contains(this.selectedCounterStorage()) === false)
                this.selectCounterStorage(this.storages().first());
        };

        counterStorages.prototype.createNewCountersStorage = function () {
            var _this = this;
            require(["viewmodels/counter/createCounterStorage"], function (createCounterStorage) {
                var createCounterStorageiewModel = new createCounterStorage(_this.storages);
                createCounterStorageiewModel.creationTask.done(function (counterStorageName, counterStoragePath) {
                    counterStoragePath = !!counterStoragePath && counterStoragePath.length > 0 ? counterStoragePath : "~/Counters/" + counterStorageName;
                    _this.showCreationAdvancedStepsIfNecessary(counterStorageName, counterStoragePath);
                });
                app.showDialog(createCounterStorageiewModel);
            });
        };

        counterStorages.prototype.showCreationAdvancedStepsIfNecessary = function (counterStorageName, counterStoragePath) {
            var _this = this;
            new createCounterStorageCommand(counterStorageName, counterStoragePath).execute().done(function () {
                return _this.storages.unshift(new counterStorage(counterStorageName));
            });
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
