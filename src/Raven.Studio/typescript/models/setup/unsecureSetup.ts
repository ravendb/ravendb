/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");

class unsecureSetup {
    static readonly localNetworks = [ "127.0.0.1", "localhost", "::1"];
    
    port = ko.observable<string>();
    ips = ko.observableArray<ipEntry>([ipEntry.forIp("127.0.0.1")]);
    publicServerUrl = ko.observable<string>();
    unsafeNetworkConfirm = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;
    shouldDisplayUnsafeModeWarning: KnockoutComputed<boolean>;
    
    constructor() {
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables() {
        this.shouldDisplayUnsafeModeWarning = ko.pureComputed(() => {
            const ips = this.ips().map(x => x.ip());
            
            return _.some(ips, x => !_.includes(unsecureSetup.localNetworks, x));
        });
    }
    
    private initValidation() {
        this.port.extend({
            number: true
        });

        this.publicServerUrl.extend({
            validUrl: true
        });
        
        this.unsafeNetworkConfirm.extend({
            validation: [
                {
                    validator: () => {
                        return !this.shouldDisplayUnsafeModeWarning() || this.unsafeNetworkConfirm();
                    },
                    message: "Confirmation is required"
                }
            ]
        });

        this.ips.extend({
            validation: [
                {
                    validator: () => this.ips().length > 0,
                    message: "Please define at least one IP address"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            port: this.port, 
            publicServerUrl: this.publicServerUrl,
            unsafeNetworkConfirm: this.unsafeNetworkConfirm,
            ips: this.ips
        })
    }

    addIpAddress() {
        this.ips.push(new ipEntry());
    }

    removeIp(ipEntry: ipEntry) {
        this.ips.remove(ipEntry);
    }
    
    toDto() : Raven.Server.Commercial.UnsecuredSetupInfo {
        return {
            PublicServerUrl: this.publicServerUrl() || undefined,
            Port: this.port() ? parseInt(this.port(), 10) : 8080,
            Addresses: this.ips().map(x => x.ip())
        }
    }
    
}

export = unsecureSetup;
