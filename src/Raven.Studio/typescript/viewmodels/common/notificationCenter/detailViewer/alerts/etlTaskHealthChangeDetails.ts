import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import alert = require("common/notifications/models/alert");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import moment = require("moment");

class etlTaskHealthChangeDetails extends abstractAlertDetails {

    view = require("views/common/notificationCenter/detailViewer/alerts/etlTaskHealthChangeDetails.html");

    previousHealthStatus: string;
    previousHealthStatusChangeAt: string;
    hasPrevious: boolean;

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const details = this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.EtlTaskHealthChangeDetails;
        this.hasPrevious = !!details.PreviousHealthStatus;
        this.previousHealthStatus = details.PreviousHealthStatus;
        this.previousHealthStatusChangeAt = details.PreviousHealthStatusChangeAt ? moment.utc(details.PreviousHealthStatusChangeAt).local().format("LLL") : null;
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertReason() === "Etl_HealthStatusChange";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new etlTaskHealthChangeDetails(alert, center));
    }
}

export = etlTaskHealthChangeDetails;
