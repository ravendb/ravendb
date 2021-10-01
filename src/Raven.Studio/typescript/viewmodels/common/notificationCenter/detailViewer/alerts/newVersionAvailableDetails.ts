import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import alert = require("common/notifications/models/alert");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import moment = require("moment");

class newVersionAvailableDetails extends abstractAlertDetails {

    view = require("views/common/notificationCenter/detailViewer/alerts/newVersionAvailableDetails.html");

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);
    }

    releaseDate = ko.pureComputed(() => {
        const details = this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.NewVersionAvailableDetails;
        return moment(details.VersionInfo.PublishedAt).format('LLL');
    });

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "Server_NewVersionAvailable";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new newVersionAvailableDetails(alert, center));
    }
}

export = newVersionAvailableDetails;
