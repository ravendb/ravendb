/// <reference path="../../../../typings/tsd.d.ts"/>

import clusterTopology = require("models/database/cluster/clusterTopology");

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

    createCanBePromotedObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            const topology = topologyProvider();
            if (!topology.leader()) {
                return false;
            }
            return this.type() === "Watcher";
        });
    }

    createCanBeDemotedObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            const topology = topologyProvider();
            if (!topology.leader()) {
                return false;
            }
            return topology.leader() !== this.tag() && (this.type() === "Member" || this.type() === "Promotable");
        });
    }

    createStateObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            const topology = topologyProvider();

            if (!topology.leader()) {
                if (this.type() === "Watcher") {
                    return "Waiting";
                } else if (this.tag() === "?") {
                    return "Passive";
                } else {
                    return "Voting";
                }
            }

            if (topology.nodeTag() !== topology.leader()) {
                return "";
            }

            return this.connected() ? "Active" : "Error";
        });
    }

    createStateClassObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            const topology = topologyProvider();
            if (!topology.leader()) {
                return this.type() === "Watcher" ? "state-default" : "state-info";
            }

            if (topology.nodeTag() !== topology.leader()) {
                return "state-unknown";
            }

            return this.connected() ? "state-success" : "state-danger";
        });
    }

}

export = clusterNode;
