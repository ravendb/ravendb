/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");

class nodeInfo {
    
    useOwnCertificates: KnockoutObservable<boolean>;
    
    nodeTag = ko.observable<string>(); //TODO: do we need it
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<number>();
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateFileName = ko.observable<string>();
    
    ipInput = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    
    
    private constructor(useOwnCertificates: KnockoutObservable<boolean>) {
        this.useOwnCertificates = useOwnCertificates;
        this.initValidation();
    }

    private initValidation() {
        //TODO: ips
        if (this.useOwnCertificates) {
            this.certificate.extend({
                required: true
            });
            
            this.nodeTag.extend({
                required: true
            })
        }
        
        this.port.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            nodeTag: this.nodeTag,
            port: this.port, 
            certificate: this.certificate,
            certificatePassword: this.certificatePassword
            //TODO: ips, 
        });
    }
    
    addIp() {
        if (this.ipInput()) {
            const entry = new ipEntry();
            entry.ip(this.ipInput());
            this.ips.push(entry);
            
            this.ipInput("");
        }
    }

    removeIp(ipEntry: ipEntry) {
        this.ips.remove(ipEntry);
    }
    
    static empty(ownCertificates: KnockoutObservable<boolean>) {
        return new nodeInfo(ownCertificates);
    }
}

export = nodeInfo;
