/// <reference path="../../../../typings/tsd.d.ts" />

import EVENTS = require("common/constants/events");
import resource = require("models/resources/resource");

abstract class abstractNotification {

    resource: resource;

    id: string;
    createdAt = ko.observable<moment.Moment>();
    isPersistent = ko.observable<boolean>();
    message = ko.observable<string>();
    title = ko.observable<string>();
    type: Raven.Server.NotificationCenter.Notifications.NotificationType;
    hasDetails: KnockoutComputed<boolean>;
    canBePostponed: KnockoutComputed<boolean>;

    constructor(rs: resource, dto: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.resource = rs;
        this.id = dto.Id;
        this.type = dto.Type;

        this.canBePostponed = ko.pureComputed(() => this.isPersistent());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.createdAt(incomingChanges.CreatedAt ? moment.utc(incomingChanges.CreatedAt) : null);
        this.isPersistent(incomingChanges.IsPersistent);
        this.message(incomingChanges.Message);
        this.title(incomingChanges.Title);
    }

}

export = abstractNotification;
