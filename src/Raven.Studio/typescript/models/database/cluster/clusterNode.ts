/// <reference path="../../../../typings/tsd.d.ts"/>

import clusterTopology = require("models/database/cluster/clusterTopology");
import generalUtils = require("common/generalUtils");
import license = require("models/auth/licenseModel");

class clusterNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<clusterNodeType>();
    connected = ko.observable<boolean>();
    utilizedCores = ko.observable<number>();
    numberOfCores = ko.observable<number>();
    installedMemoryInGb = ko.observable<number>();
    installedMemory = ko.pureComputed(() => this.getNumber(this.installedMemoryInGb()));
    usableMemoryInGb = ko.observable<number>();
    usableMemory = ko.pureComputed(() => this.getNumber(this.usableMemoryInGb()));
    errorDetails = ko.observable<string>();
    isLeader = ko.observable<boolean>();
    isPassive: KnockoutObservable<boolean>;
    nodeServerVersion = ko.observable<string>();
    
    constructor(isPassive: KnockoutObservable<boolean>) {
        this.isPassive = isPassive;
    }
    
    errorDetailsShort = ko.pureComputed(() => {
        const longError = this.errorDetails();
        return generalUtils.trimMessage(longError);
    });

    utilizedMemoryInGb = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        let utilizedMemory = this.utilizedCores() * licenseStatus.Ratio;
        const installedMemoryInGb = this.installedMemoryInGb();
        utilizedMemory = utilizedMemory > installedMemoryInGb ? installedMemoryInGb : utilizedMemory;
        return this.getNumber(utilizedMemory);
    });

    cssCores = ko.pureComputed(() => {
        if (this.utilizedCores() >= this.numberOfCores()) {
            return "text-success";
        }

        return "text-warning";
    });

    cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":                
                if (this.isLeader()) {
                    return "icon-node-leader";
                } else {
                    return "icon-cluster-member";
                }
                
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
        this.utilizedCores(incoming.utilizedCores());
        this.numberOfCores(incoming.numberOfCores());
        this.installedMemoryInGb(incoming.installedMemoryInGb());
        this.usableMemoryInGb(incoming.usableMemoryInGb());
        this.errorDetails(incoming.errorDetails());
        this.isLeader(incoming.isLeader());
        this.nodeServerVersion(incoming.nodeServerVersion());
    }

    static for(tag: string, serverUrl: string, type: clusterNodeType, connected: boolean, isPassive: KnockoutObservable<boolean>, errorDetails?: string) {
        const node = new clusterNode(isPassive);
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
                } else if (this.isPassive()) {
                    return "Passive";
                } else {
                    return this.connected() ? "Voting" : "Error";
                }
            }

            return this.connected() ? "Active" : "Error";
        });
    }

    createStateClassObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            
            const topology = topologyProvider();
            if (!topology.leader()) {
                if (this.type() === "Watcher") {
                    return "state-default";
                }
                
                return this.connected() ? "state-info" : "state-danger";
            }

            return this.connected() ? "state-success" : "state-danger";
        });
    }

    private getNumber(num: number): string {
        if (Number.isInteger(num)) {
            return num.toString();
        }

        return num.toFixed(2);
    }
}

export = clusterNode;
