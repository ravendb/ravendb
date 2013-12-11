define(["require", "exports", "common/raven", "common/alertArgs", "common/alertType"], function(require, exports, raven, alertArgs, alertType) {
    /// Commands encapsulate a write operation to the database and support progress notifications.
    var commandBase = (function () {
        function commandBase() {
            this.ravenDb = new raven();
        }
        commandBase.prototype.execute = function () {
            throw new Error("Execute must be overridden.");
        };

        commandBase.prototype.reportInfo = function (title, details) {
            this.reportProgress(0 /* info */, title, details);
        };

        commandBase.prototype.reportError = function (title, details) {
            this.reportProgress(3 /* danger */, title, details);
        };

        commandBase.prototype.reportSuccess = function (title, details) {
            this.reportProgress(1 /* success */, title, details);
        };

        commandBase.prototype.reportWarning = function (title, details) {
            this.reportProgress(2 /* warning */, title, details);
        };

        commandBase.prototype.reportProgress = function (type, title, details) {
            ko.postbox.publish("Alert", new alertArgs(type, title, details));
        };
        return commandBase;
    })();

    
    return commandBase;
});
//# sourceMappingURL=commandBase.js.map
