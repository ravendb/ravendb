import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");

abstract class virtualNotification extends abstractNotification {

    protected constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.Notification) {
        super(db, dto);
        
        this.requiresRemoteDismiss(false);
    }
}

export = virtualNotification;
