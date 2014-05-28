var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "models/resource"], function(require, exports, resource) {
    var counterStorage = (function (_super) {
        __extends(counterStorage, _super);
        function counterStorage(name) {
            _super.call(this, name, 'counterstorage');
            this.name = name;
            this.name = name;
        }
        counterStorage.prototype.activate = function () {
            ko.postbox.publish("ActivateCounterStorage", this);
        };
        return counterStorage;
    })(resource);

    
    return counterStorage;
});
//# sourceMappingURL=counterStorage.js.map
