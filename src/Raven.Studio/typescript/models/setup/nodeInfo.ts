/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");

class nodeInfo {
    
    nodeTag = ko.observable<string>();
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<string>();
    serverUrl = ko.observable<string>(); //TODO: validation, binding etc
    
    validationGroup: KnockoutValidationGroup;
    
    constructor() {
        this.initValidation();
        
        this.ips.push(new ipEntry());
    }

    private initValidation() {
        this.port.extend({
            required: true,
            number: true
        });
        
        this.ips.extend({
            validation: [
                {
                    validator: () => this.ips().length > 0,
                    message: "Please define at least one IP for this node"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            nodeTag: this.nodeTag,
            port: this.port, 
            ips: this.ips
        });
    }

    addIpAddress() {
        this.ips.push(new ipEntry());
    }

    removeIp(ipEntry: ipEntry) {
        this.ips.remove(ipEntry);
    }

    toDto(): Raven.Server.Commercial.SetupInfo.NodeInfo {
        return {
            Ips: this.ips().map(x => x.ip()),
            Port: parseInt(this.port(), 10),
            ServerUrl: this.serverUrl()
        };
    }
}

export = nodeInfo;
