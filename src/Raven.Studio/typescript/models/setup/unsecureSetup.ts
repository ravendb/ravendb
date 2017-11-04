/// <reference path="../../../typings/tsd.d.ts"/>

type configurationMode = "Unsecured" | "Secured" | "LetsEncrypt"; //tODO use enum SetupMode

class unsecureSetup {
    static readonly localNetworks = [ "127.0.0.1", "localhost", "::1"];
    
    serverUrl = ko.observable<string>(location.origin);
    publicServerUrl = ko.observable<string>();
    unsafeNetworkConfirm = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;
    
    shouldDisplayUnsafeModeWarning: KnockoutComputed<boolean>;
    
    constructor() {
        this.initObservables();
        this.initValidation();
        
        this.unsafeNetworkConfirm.subscribe(v => console.log("value = " + v));
    }
    
    private initObservables() {
        this.shouldDisplayUnsafeModeWarning = ko.pureComputed(() => {
            const serverUrlIsValid = this.serverUrl.isValid();
            if (!serverUrlIsValid) {
                return false;
            }
            
            try {
                const hostname = new URL(this.serverUrl()).hostname;
                return !_.includes(unsecureSetup.localNetworks, hostname);
            } catch (e) {
                return false;
            }
        });
    }
    
    private initValidation() {
        this.serverUrl.extend({
            required: true,
            validUrl: true
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
        
        this.validationGroup = ko.validatedObservable({
            serverUrl: this.serverUrl,
            publicServerUrl: this.publicServerUrl,
            unsafeNetworkConfirm: this.unsafeNetworkConfirm
        })
    }
    
    toDto() : Raven.Server.Commercial.UnsecuredSetupInfo {
        return {
            PublicServerUrl: this.publicServerUrl() || undefined,
            ServerUrl: this.serverUrl()
        }
    }
    
}

export = unsecureSetup;
