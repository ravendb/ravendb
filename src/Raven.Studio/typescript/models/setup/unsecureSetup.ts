/// <reference path="../../../typings/tsd.d.ts"/>

type configurationMode = "Unsecured" | "Secured" | "LetsEncrypt"; //tODO use enum SetupMode

class unsecureSetup {
    serverUrl = ko.observable<string>();
    publicServerUrl = ko.observable<string>();
    unsafeNetworkConfirm = ko.observable<boolean>(false);
    
    //TODO: validation etc.
    
    toDto() : Raven.Server.Commercial.UnsecuredSetupInfo {
        return {
            PublicServerUrl: this.publicServerUrl() || undefined,
            ServerUrl: this.serverUrl()
        }
    }
    
}

export = unsecureSetup;
