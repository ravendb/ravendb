var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "viewmodels/viewModelBase"], function(require, exports, viewModelBase) {
    var counterStorageConfiguration = (function (_super) {
        __extends(counterStorageConfiguration, _super);
        function counterStorageConfiguration() {
            _super.apply(this, arguments);
        }
        counterStorageConfiguration.prototype.canActivate = function (args) {
            return true;
        };
        return counterStorageConfiguration;
    })(viewModelBase);

    
    return counterStorageConfiguration;
});
//# sourceMappingURL=counterStorageConfiguration.js.map
