define(["require", "exports"], function(require, exports) {
    var query = (function () {
        function query() {
            this.displayName = "query";
        }
        query.prototype.activate = function () {
        };
        query.prototype.canDeactivate = function () {
            return true;
        };
        return query;
    })();
    exports.query = query;
});
//# sourceMappingURL=query.js.map
