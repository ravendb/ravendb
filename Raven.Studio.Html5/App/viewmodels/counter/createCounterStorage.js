var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "plugins/dialog", "viewmodels/dialogViewModelBase"], function(require, exports, dialog, dialogViewModelBase) {
    var createCounterStorage = (function (_super) {
        __extends(createCounterStorage, _super);
        function createCounterStorage(counterStorages) {
            _super.call(this);
            this.creationTask = $.Deferred();
            this.creationTaskStarted = false;
            this.counterStorageName = ko.observable('');
            this.counterStoragePath = ko.observable('');
            this.counterStorages = ko.observableArray();
            this.maxNameLength = 200;
            this.counterStorages = counterStorages;
        }
        createCounterStorage.prototype.attached = function () {
            var _this = this;
            var inputElement = $("#counterStorageName")[0];
            this.counterStorageName.subscribe(function (newCounterStorageName) {
                var errorMessage = '';

                if (_this.isCounterStorageNameExists(newCounterStorageName.toLowerCase(), _this.counterStorages())) {
                    errorMessage = "Database Name Already Exists!";
                } else if ((errorMessage = _this.CheckName(newCounterStorageName)) != '') {
                }
                inputElement.setCustomValidity(errorMessage);
            });
            this.subscribeToPath("#databasePath", this.counterStoragePath, "Path");
        };

        createCounterStorage.prototype.deactivate = function () {
            // If we were closed via X button or other dialog dismissal, reject the deletion task since
            // we never started it.
            if (!this.creationTaskStarted) {
                this.creationTask.reject();
            }
        };

        createCounterStorage.prototype.cancel = function () {
            dialog.close(this);
        };

        createCounterStorage.prototype.nextOrCreate = function () {
            var counterStorageName = this.counterStorageName();

            this.creationTaskStarted = true;
            this.creationTask.resolve(this.counterStorageName(), this.counterStoragePath());
            dialog.close(this);
        };

        createCounterStorage.prototype.isCounterStorageNameExists = function (databaseName, counterStorages) {
            for (var i = 0; i < counterStorages.length; i++) {
                if (databaseName == counterStorages[i].name.toLowerCase()) {
                    return true;
                }
            }
            return false;
        };

        createCounterStorage.prototype.CheckName = function (name) {
            var rg1 = /^[^\\/\*:\?"<>\|]+$/;
            var rg2 = /^\./;
            var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i;

            var message = '';
            if (!$.trim(name)) {
                message = "An empty counter storage name is forbidden for use!";
            } else if (name.length > this.maxNameLength) {
                message = "The counter storage length can't exceed " + this.maxNameLength + " characters!";
            } else if (!rg1.test(name)) {
                message = "The counter storage name can't contain any of the following characters: \ / * : ?" + ' " ' + "< > |";
            } else if (rg2.test(name)) {
                message = "The counter storage name can't start with a dot!";
            } else if (rg3.test(name)) {
                message = "The name '" + name + "' is forbidden for use!";
            }
            return message;
        };

        createCounterStorage.prototype.subscribeToPath = function (tag, element, pathName) {
            var _this = this;
            var inputElement = $(tag)[0];
            element.subscribe(function (path) {
                var errorMessage = _this.isPathLegal(path, pathName);
                inputElement.setCustomValidity(errorMessage);
            });
        };

        createCounterStorage.prototype.isPathLegal = function (name, pathName) {
            var rg1 = /^[^\\*:\?"<>\|]+$/;
            var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i;
            var errorMessage = null;

            if (!$.trim(name) == false) {
                if (name.length > 30) {
                    errorMessage = "The path name for the '" + pathName + "' can't exceed " + 30 + " characters!";
                } else if (!rg1.test(name)) {
                    errorMessage = "The " + pathName + " can't contain any of the following characters: * : ?" + ' " ' + "< > |";
                } else if (rg2.test(name)) {
                    errorMessage = "The name '" + name + "' is forbidden for use!";
                }
            }
            return errorMessage;
        };
        return createCounterStorage;
    })(dialogViewModelBase);

    
    return createCounterStorage;
});
//# sourceMappingURL=createCounterStorage.js.map
