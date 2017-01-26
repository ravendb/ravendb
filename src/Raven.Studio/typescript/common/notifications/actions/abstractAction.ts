/// <reference path="../../../../typings/tsd.d.ts" />

import EVENTS = require("common/constants/events");
import resource = require("models/resources/resource");

abstract class abstractAction {

    resource: resource;

    id: string;
    createdAt = ko.observable<string>();//TODO: use moment for easy sort?
    details = ko.observable<Raven.Server.NotificationCenter.Actions.Details.IActionDetails>();
    isPersistent = ko.observable<boolean>();
    message = ko.observable<string>();
    title = ko.observable<string>();
    type: Raven.Server.NotificationCenter.Actions.ActionType;
    hasDetails: KnockoutComputed<boolean>;
    canBePostponed: KnockoutComputed<boolean>;

    constructor(rs: resource, dto: Raven.Server.NotificationCenter.Actions.Action) {
        this.resource = rs;
        this.id = dto.Id;
        this.type = dto.Type;

        this.hasDetails = ko.pureComputed(() => !!this.details());
        this.canBePostponed = ko.pureComputed(() => this.isPersistent());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Actions.Action) {
        this.createdAt(incomingChanges.CreatedAt);
        this.details(incomingChanges.Details);
        this.isPersistent(incomingChanges.IsPersistent);
        this.message(incomingChanges.Message);
        this.title(incomingChanges.Title);
    }

}

export = abstractAction;
