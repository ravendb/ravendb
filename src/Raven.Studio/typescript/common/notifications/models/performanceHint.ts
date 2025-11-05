/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");

class performanceHint extends abstractNotification {

    details = ko.observable<Raven.Server.NotificationCenter.Notifications.Details.INotificationDetails>();
    source = ko.observable<string>();
    performanceHintReason = ko.observable<Raven.Server.NotificationCenter.Notifications.PerformanceHintReason>();
    dontShowAgain = ko.observable<boolean>(false);

    constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.PerformanceHint) {
        super(db, dto);

        this.updateWith(dto);
        this.hasDetails = ko.pureComputed(() => !!this.details());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.PerformanceHint) {
        super.updateWith(incomingChanges);

        this.details(incomingChanges.Details);
        this.source(incomingChanges.Source);
        this.performanceHintReason(incomingChanges.Reason);
    }

}

export = performanceHint;
