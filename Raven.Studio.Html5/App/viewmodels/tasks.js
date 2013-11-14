define(["require", "exports"], function(require, exports) {
    var tasks = (function () {
        function tasks() {
            this.displayName = "tasks";
        }
        tasks.prototype.activate = function () {
        };
        tasks.prototype.canDeactivate = function () {
            return true;
        };
        return tasks;
    })();
    exports.tasks = tasks;
});
//# sourceMappingURL=tasks.js.map
