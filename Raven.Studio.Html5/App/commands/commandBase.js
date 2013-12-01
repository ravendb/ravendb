define(["require", "exports", "common/raven", "common/alertArgs", "common/alertType"], function(require, exports, __raven__, __alertArgs__, __alertType__) {
    var raven = __raven__;
    var alertArgs = __alertArgs__;
    var alertType = __alertType__;

    /// Commands encapsulate a write operation to the database and support progress notifications.
    var commandBase = (function () {
        function commandBase() {
            this.ravenDb = new raven();
        }
        commandBase.prototype.execute = function () {
            throw new Error("Execute must be overridden.");
        };

        commandBase.prototype.reportInfo = function (title, details) {
            this.reportProgress(alertType.info, title, details);
        };

        commandBase.prototype.reportError = function (title, details) {
            this.reportProgress(alertType.danger, title, details);
        };

        commandBase.prototype.reportSuccess = function (title, details) {
            this.reportProgress(alertType.success, title, details);
        };

        commandBase.prototype.reportWarning = function (title, details) {
            this.reportProgress(alertType.warning, title, details);
        };

        commandBase.prototype.reportProgress = function (type, title, details) {
            ko.postbox.publish("Alert", new alertArgs(type, title, details));
        };
        return commandBase;
    })();

    
    return commandBase;
});
//# sourceMappingURL=commandBase.js.map
