var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "viewmodels/viewModelBase"], function(require, exports, viewModelBase) {
    var counterStorageReplication = (function (_super) {
        __extends(counterStorageReplication, _super);
        function counterStorageReplication() {
            _super.apply(this, arguments);
        }
        counterStorageReplication.prototype.canActivate = function (args) {
            return true;
        };
        return counterStorageReplication;
    })(viewModelBase);

    
    return counterStorageReplication;
});
//# sourceMappingURL=counterStorageReplication.js.map
