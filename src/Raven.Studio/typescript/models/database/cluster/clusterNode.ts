/// <reference path="../../../../typings/tsd.d.ts"/>

class clusterNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<clusterNodeType>();
    connected = ko.observable<boolean>();
    errorDetails = ko.observable<string>();

    cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":
                return "icon-cluster-member";
            case "Promotable":
                return "icon-cluster-promotable";
            case "Watcher":
                return "icon-cluster-watcher";
        }
    });

    canBePromoted: KnockoutComputed<boolean>;
    canBeDemoted: KnockoutComputed<boolean>;

    constructor() {
        this.canBeDemoted = ko.pureComputed(() => this.type() === "Member" || this.type() === "Promotable");
        this.canBePromoted = ko.pureComputed(() => this.type() === "Watcher");
    }

    updateWith(incoming: clusterNode) {
        this.tag(incoming.tag());
        this.type(incoming.type());
        this.connected(incoming.connected());
        this.errorDetails(incoming.errorDetails());
    }

    static for(tag: string, serverUrl: string, type: clusterNodeType, connected: boolean, errorDetails?: string) {
        const node = new clusterNode();
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.type(type);
        node.connected(connected);
        node.errorDetails(errorDetails);
        return node;
    }
}

export = clusterNode;
