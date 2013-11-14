define(["require", "exports"], function(require, exports) {
    var statistics = (function () {
        function statistics() {
            this.displayName = "statistics";
        }
        statistics.prototype.activate = function (args) {
            console.log("this is STATISTICS!");
        };

        statistics.prototype.canDeactivate = function () {
            return true;
        };
        return statistics;
    })();

    
    return statistics;
});
//# sourceMappingURL=statistics.js.map
