/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import resource = require("models/resources/resource");

class performanceHint extends abstractNotification {

    details = ko.observable<Raven.Server.NotificationCenter.Notifications.Details.INotificationDetails>();
    source = ko.observable<string>();
    hintType = ko.observable<Raven.Server.NotificationCenter.Notifications.PerformanceHintType>();
    dontShowAgain = ko.observable<boolean>(false);

    constructor(resource: resource, dto: Raven.Server.NotificationCenter.Notifications.PerformanceHint) {
        super(resource, dto);

        this.updateWith(dto);
        this.hasDetails = ko.pureComputed(() => !!this.details());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.PerformanceHint) {
        super.updateWith(incomingChanges);

        this.details(incomingChanges.Details);
        this.source(incomingChanges.Source);
        this.hintType(incomingChanges.HintType);
    }

}

export = performanceHint;
