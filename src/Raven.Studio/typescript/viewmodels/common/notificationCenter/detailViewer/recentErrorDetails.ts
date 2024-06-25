import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import recentError = require("common/notifications/models/recentError");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard from "common/copyToClipboard";

class recentErrorDetails extends dialogViewModelBase {

    view = require("views/common/notificationCenter/detailViewer/recentErrorDetails.html");

    protected readonly recentError: recentError;
    protected readonly dismissFunction: () => void;

    constructor(recentError: recentError, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close", "dismiss", "copyErrorDetails");

        this.recentError = recentError;
        this.dismissFunction = () => notificationCenter.dismiss(recentError);
    }

    dismiss() {
        this.dismissFunction();
        this.close();
    }

    copyErrorDetails(): void {
        const errorDetails = this.recentError.details();
        copyToClipboard.copy(errorDetails, "Error has been copied to clipboard");
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification instanceof recentError;
    }

    static showDetailsFor(recentError: recentError, center: notificationCenter) {
        return app.showBootstrapDialog(new recentErrorDetails(recentError, center));
    }
}

export = recentErrorDetails;
