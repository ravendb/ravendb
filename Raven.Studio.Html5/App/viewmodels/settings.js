define(["require", "exports"], function(require, exports) {
    var settings = (function () {
        function settings() {
            this.displayName = "settings";
        }
        settings.prototype.activate = function () {
        };
        settings.prototype.canDeactivate = function () {
            return true;
        };
        return settings;
    })();
    exports.settings = settings;
});
//# sourceMappingURL=settings.js.map
