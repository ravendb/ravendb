import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import recentError = require("common/notifications/models/recentError");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import alert = require("common/notifications/models/alert");

class licenseLimitDetails extends dialogViewModelBase {

    protected readonly licenseLimitNotification: recentError | alert;
    protected readonly dismissFunction: () => void;

    constructor(licenseLimitNotification: alert | recentError, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close");

        this.licenseLimitNotification = licenseLimitNotification;
    }

    static supportsDetailsFor(notification: abstractNotification) {
        const isLicenceAlert = (notification instanceof alert) && notification.isLicenseAlert(); 
        const isRecentLicenseError = (notification instanceof recentError) && notification.details() === recentError.licenceLimitMarker;
        
        return isLicenceAlert || isRecentLicenseError;
    }

    static showDetailsFor(licenseError: recentError | alert, center: notificationCenter) {
        return app.showBootstrapDialog(new licenseLimitDetails(licenseError, center));
    }
}

export = licenseLimitDetails;
