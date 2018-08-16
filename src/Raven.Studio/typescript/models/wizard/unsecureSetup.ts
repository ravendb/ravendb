/// <reference path="../../../typings/tsd.d.ts"/>
import ipEntry = require("models/wizard/ipEntry");

class unsecureSetup {

    port = ko.observable<string>();
    tcpPort = ko.observable<string>();
    ip = ko.observable<ipEntry>();
    unsafeNetworkConfirm = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;
    shouldDisplayUnsafeModeWarning: KnockoutComputed<boolean>;
    
    constructor() {
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables() {
        this.shouldDisplayUnsafeModeWarning = ko.pureComputed(() => {
            if (!this.ip()) {
                return false;
            }
            return !this.ip().isLocalNetwork();
        });
    }
    
    private initValidation() {
        this.port.extend({
            number: true
        });
        
        this.tcpPort.extend({
            number: true
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

        this.validationGroup = ko.validatedObservable({
            port: this.port, 
            unsafeNetworkConfirm: this.unsafeNetworkConfirm,
            tcpPort: this.tcpPort
        })
    }
    
}

export = unsecureSetup;
