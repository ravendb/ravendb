import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import alertArgs = require("common/alertArgs");

//TODO: integrate to notification center
class recentErrors extends dialogViewModelBase {
    constructor(private errors: KnockoutObservableArray<alertArgs>) {
        super();
    }

    attached() {
        super.attached();
        // Expand the first error.
        if (this.errors().length > 0) {
            $("#errorDetailsCollapse0").collapse("show");
        }
    }

    close() {
        dialog.close(this);
    }

    getErrorDetails(alert: alertArgs) {
        var error = alert.errorInfo;
        if (error != null && error.stackTrace) {
            return error.stackTrace.replace("\r\n", "\n");
        }

        return alert.details;
    }

    getErrorAlertType() {
        return "Error" as alertType;
    }

    getWarningAlertType() {
        return "Warning" as alertType;
    }
}

export = recentErrors; 
