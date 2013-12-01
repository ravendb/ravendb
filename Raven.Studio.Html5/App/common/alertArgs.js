define(["require", "exports", "common/alertType"], function(require, exports, __alertType__) {
    var alertType = __alertType__;

    var alertArgs = (function () {
        function alertArgs(type, title, details) {
            this.type = type;
            this.title = title;
            this.details = details;
            this.id = "alert_" + new Date().getMilliseconds().toString() + "_" + title.length.toString() + "_" + (details ? details.length.toString() : '0');
        }
        return alertArgs;
    })();

    
    return alertArgs;
});
//# sourceMappingURL=alertArgs.js.map
