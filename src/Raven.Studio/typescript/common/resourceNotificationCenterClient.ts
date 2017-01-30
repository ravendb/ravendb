/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");
import EVENTS = require("common/constants/events");
import endpoints = require("endpoints");

import abstractNotificationCenterClient = require("common/abstractNotificationCenterClient");

class resourceNotificationCenterClient extends abstractNotificationCenterClient {

    constructor(rs: resource) {
        super(rs);
    }

    get connectionDescription() {
        return "Notification Center Client: " + this.rs.qualifiedName;
    }

    protected webSocketUrlFactory(token: singleAuthToken) {
        const connectionString = "?singleUseAuthToken=" + token.Token;
        return endpoints.databases.databaseNotificationCenter.notificationCenterWatch + connectionString;
    }

}

export = resourceNotificationCenterClient;

