/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");

class nodeInfo {
    
    nodeTag = ko.observable<string>();
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<string>();
    hostname = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    
    private hostnameIsRequired: () => boolean;
    
    constructor(hostnameIsRequired: () => boolean) {
        this.hostnameIsRequired = hostnameIsRequired;
        this.initValidation();
        
        this.ips.push(new ipEntry());
    }

    private initValidation() {
        this.port.extend({
            required: true,
            number: true
        });
        
        this.hostname.extend({
            requried: {
                onlyIf: () => this.hostnameIsRequired()
            }
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
            ips: this.ips,
            serverUrl: this.hostname
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
            ServerUrl: "https://" + this.hostname() + ":" + this.port()
        };
    }
}

export = nodeInfo;
