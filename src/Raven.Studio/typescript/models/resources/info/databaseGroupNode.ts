/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");

class databaseGroupNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<databaseGroupNodeType>();
    responsibleNode = ko.observable<string>();

    lastStatus = ko.observable<Raven.Client.ServerWide.DatabasePromotionStatus>();
    lastError = ko.observable<string>();
    lastErrorShort = ko.pureComputed(() => {
        const longError = this.lastError();
        return generalUtils.trimMessage(longError);
    });

    cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":
                return "icon-dbgroup-member";
            case "Promotable":
                return "icon-dbgroup-promotable";
            case "Rehab":
                return "icon-dbgroup-rehab";
        }
        return "";
    });

    badgeClass = ko.pureComputed(() => {
        switch (this.lastStatus()) {
            case "Ok":
                return "state-success";
            case "NotResponding":
                return "state-danger";
            default:
                return "state-warning";
        }
    });

    badgeText = ko.pureComputed(() => {
        switch (this.lastStatus()) {
            case "Ok":
                return "Active";
            case "NotResponding":
                return "Error";
            default:
                return "Catching up";
        }
    });

    static for(tag: string, serverUrl: string, responsibleNode: string, type: databaseGroupNodeType) {
        const node = new databaseGroupNode();
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.responsibleNode(responsibleNode);
        node.type(type);
        return node;
    }
    
    update(incoming: databaseGroupNode) {
        this.tag(incoming.tag());
        this.serverUrl(incoming.serverUrl());
        this.type(incoming.type());
        this.responsibleNode(incoming.responsibleNode());
        this.lastStatus(incoming.lastStatus());
        this.lastError(incoming.lastError());
    }
}

export = databaseGroupNode;
