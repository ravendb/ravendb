/// <reference path="../../../typings/tsd.d.ts"/>
import ipEntry = require("models/wizard/ipEntry");

class nodeInfo {
    
    nodeTag = ko.observable<string>();
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<string>();
    hostname = ko.observable<string>();
    isLocal: KnockoutComputed<boolean>;
    externalIpAddress = ko.observable<string>();
    
    effectiveIpAddress: KnockoutComputed<string>;
    validationGroup: KnockoutValidationGroup;
    
    showAdvancedSettings = ko.observable<boolean>(false);
    
    private hostnameIsOptional: KnockoutObservable<boolean>;
    
    constructor(hostnameIsOptional: KnockoutObservable<boolean>) {
        this.hostnameIsOptional = hostnameIsOptional;
        this.initValidation();
        
        this.isLocal = ko.pureComputed(() => {
            return this.nodeTag() === 'A';
        });
        
        this.ips.push(new ipEntry());
        
        this.effectiveIpAddress = ko.pureComputed(() => {
           const externalIp = this.externalIpAddress();
           if (externalIp && this.showAdvancedSettings()) {
               return externalIp;
           }
           
           if (this.ips().length) {
               return this.ips()[0].ip();
           }
           
           return "";
        });
    }

    private initValidation() {
        this.port.extend({
            number: true
        });
        
        this.externalIpAddress.extend({
            validIpAddress: true
        });
        
        this.hostname.extend({
            validation: [{
                validator: (val: string) => this.hostnameIsOptional() || _.trim(val),
                message: "This field is required"
            }]
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
            hostname: this.hostname,
            externalIpAddress: this.externalIpAddress
        });
    }

    addIpAddress() {
        this.ips.push(new ipEntry());
    }

    removeIp(ipEntry: ipEntry) {
        this.ips.remove(ipEntry);
    }
    
    getServerUrl() {
        if (!this.hostname()) {
            return null;
        }
        
        let serverUrl = "https://" + this.hostname();
        if (this.port() && this.port() !== "443") {
            serverUrl += ":" + this.port();
        }
        return serverUrl;
    }

    toDto(): Raven.Server.Commercial.SetupInfo.NodeInfo {
        return {
            Addresses: this.ips().map(x => x.ip()),
            Port: this.port() ? parseInt(this.port(), 10) : null,
            PublicServerUrl: this.getServerUrl(),
            ExternalIpAddress: this.externalIpAddress() || null
        };
    }
}

export = nodeInfo;
