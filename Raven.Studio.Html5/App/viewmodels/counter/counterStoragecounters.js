var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "viewmodels/viewModelBase"], function(require, exports, viewModelBase) {
    var counterStoragecounters = (function (_super) {
        __extends(counterStoragecounters, _super);
        function counterStoragecounters() {
            _super.apply(this, arguments);
        }
        counterStoragecounters.prototype.canActivate = function (args) {
            return true;
        };
        return counterStoragecounters;
    })(viewModelBase);

    
    return counterStoragecounters;
});
//# sourceMappingURL=counterStoragecounters.js.map
