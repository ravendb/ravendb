var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "plugins/dialog", "viewmodels/dialogViewModelBase", "models/counter/counter"], function(require, exports, dialog, dialogViewModelBase, counter) {
    var editCounterDialog = (function (_super) {
        __extends(editCounterDialog, _super);
        function editCounterDialog(editedCounter) {
            _super.call(this);
            this.updateTask = $.Deferred();
            this.updateTaskStarted = false;
            this.isNewCounter = ko.observable(false);
            this.editedCounter = ko.observable();
            this.counterDelta = ko.observable(0);
            this.maxNameLength = 200;

            if (!editedCounter) {
                this.isNewCounter(true);
                this.editedCounter(new counter({ Name: '', Group: '', OverallTotal: 0, Servers: [] }));
            } else {
                this.editedCounter(editedCounter);
            }
            this.counterDelta(0);
        }
        editCounterDialog.prototype.cancel = function () {
            dialog.close(this);
        };

        editCounterDialog.prototype.nextOrCreate = function () {
            this.updateTaskStarted = true;
            this.updateTask.resolve(this.editedCounter(), this.counterDelta());
            dialog.close(this);
        };

        editCounterDialog.prototype.attached = function () {
            var _this = this;
            this.counterDelta(0);
            var inputElement = $("#counterId")[0];
            this.editedCounter().id.subscribe(function (newCounterId) {
                var errorMessage = '';

                if ((errorMessage = _this.CheckName(newCounterId, 'counter name')) != '') {
                }
                inputElement.setCustomValidity(errorMessage);
            });
            this.editedCounter().group.subscribe(function (newCounterId) {
                var errorMessage = '';

                if ((errorMessage = _this.CheckName(newCounterId, 'group name')) != '') {
                }
                inputElement.setCustomValidity(errorMessage);
            });
            //todo: maybe check validity of delta
        };

        editCounterDialog.prototype.deactivate = function () {
            // If we were closed via X button or other dialog dismissal, reject the deletion task since
            // we never started it.
            if (!this.updateTaskStarted) {
                this.updateTask.reject();
            }
        };

        editCounterDialog.prototype.CheckName = function (name, fieldName) {
            var message = '';
            if (!$.trim(name)) {
                message = "An empty " + fieldName + " is forbidden for use!";
            } else if (name.length > this.maxNameLength) {
                message = "The  " + fieldName + " length can't exceed " + this.maxNameLength + " characters!";
            }
            return message;
        };
        return editCounterDialog;
    })(dialogViewModelBase);

    
    return editCounterDialog;
});
//# sourceMappingURL=editCounterDialog.js.map
