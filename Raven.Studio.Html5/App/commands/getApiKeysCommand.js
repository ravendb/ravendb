var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/apiKey"], function(require, exports, commandBase, apiKey) {
    var getApiKeysCommand = (function (_super) {
        __extends(getApiKeysCommand, _super);
        function getApiKeysCommand() {
            _super.apply(this, arguments);
        }
        getApiKeysCommand.prototype.execute = function () {
            var args = {
                startsWith: "Raven/ApiKeys/",
                exclude: null,
                start: 0,
                pageSize: 256
            };

            return this.query("/docs", args, null, function (dtos) {
                return dtos.map(function (dto) {
                    return new apiKey(dto);
                });
            });
        };
        return getApiKeysCommand;
    })(commandBase);

    
    return getApiKeysCommand;
});
//# sourceMappingURL=getApiKeysCommand.js.map
