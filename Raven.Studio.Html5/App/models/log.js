define(["require", "exports"], function(require, exports) {
    var log = (function () {
        function log(dto) {
            this.isSelected = ko.observable(false);
            this.message = dto.Message;
            this.exception = dto.Exception;
            this.level = dto.Level;
            this.timeStamp = dto.TimeStamp;
            this.loggerName = dto.LoggerName;
            this.timeStampText = null;
        }
        return log;
    })();

    
    return log;
});
//# sourceMappingURL=log.js.map
