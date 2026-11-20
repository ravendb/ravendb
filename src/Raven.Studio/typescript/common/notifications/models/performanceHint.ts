/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");
import notificationDetailsByReason = require("common/notifications/models/notificationDetailsByReason");

type PerformanceHintReason = Raven.Server.NotificationCenter.Notifications.PerformanceHintReason;
type PerformanceHintDetails<TReason extends PerformanceHintReason> =
    notificationDetailsByReason.PerformanceHintDetails<TReason>;

class performanceHint<TReason extends PerformanceHintReason = PerformanceHintReason> extends abstractNotification {

    details = ko.observable<PerformanceHintDetails<TReason>>();
    source = ko.observable<string>();
    performanceHintReason = ko.observable<TReason>();
    dontShowAgain = ko.observable<boolean>(false);

    constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.PerformanceHint) {
        super(db, dto);

        this.updateWith(dto);
        this.hasDetails = ko.pureComputed(() => !!this.details());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.PerformanceHint) {
        super.updateWith(incomingChanges);

        this.details(incomingChanges.Details as PerformanceHintDetails<TReason>);
        this.source(incomingChanges.Source);
        this.performanceHintReason(incomingChanges.Reason as TReason);
    }

}

export = performanceHint;
