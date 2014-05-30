var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "viewmodels/viewModelBase"], function(require, exports, viewModelBase) {
    var counterStorageStats = (function (_super) {
        __extends(counterStorageStats, _super);
        function counterStorageStats() {
            _super.apply(this, arguments);
        }
        counterStorageStats.prototype.canActivate = function (args) {
            return true;
        };
        return counterStorageStats;
    })(viewModelBase);

    
    return counterStorageStats;
});
//# sourceMappingURL=counterStorageStats.js.map
