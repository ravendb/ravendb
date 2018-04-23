import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerCertificateSetupModeCommand extends commandBase { 
    
    execute(): JQueryPromise<Raven.Server.Commercial.SetupMode> {
       
        const url = endpoints.global.adminCertificates.adminCertificatesMode;
        
        return this.query(url, null, null, x => x.SetupMode)
            .fail((response: JQueryXHR) => this.reportError("Failed to get the server certificate setup mode", response.responseText, response.statusText));
    }
}

export = getServerCertificateSetupModeCommand;
