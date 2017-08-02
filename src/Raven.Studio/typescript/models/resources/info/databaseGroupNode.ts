/// <reference path="../../../../typings/tsd.d.ts"/>

class databaseGroupNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<databaseGroupNodeType>();
    responsibleNode = ko.observable<string>();

    lastStatus = ko.observable<string>();
    lastError = ko.observable<string>();

    cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":
                return "icon-dbgroup-member";
            case "Promotable":
                return "icon-dbgroup-promotable";
            case "Watcher":
                return "icon-dbgroup-watcher";
            case "Rehab":
                return "icon-dbgroup-rehab";
        }
        return "";
    });

    badgeClass = ko.pureComputed(() => {
        return this.lastStatus() === "Ok" ? "state-success" : "state-danger";
    });

    badgeText = ko.pureComputed(() => {
        //TODO: update me
        return this.lastStatus() === "Ok" ? "Active" : "Invalid";
    });

    static for(tag: string, serverUrl: string, responsibleNode: string, type: databaseGroupNodeType) {
        const node = new databaseGroupNode();
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.responsibleNode(responsibleNode);
        node.type(type);
        return node;
    }
}

export = databaseGroupNode;
