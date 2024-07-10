import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getSetupLocalNodeIpsCommand extends commandBase {

    execute(): JQueryPromise<string[]> {      
        const url = endpoints.global.setup.setupIps;
        
        return this.query<string[]>(url, null, null, x => x.NetworkInterfaces.flatMap((i: any) => i.Addresses))
            .fail((response: JQueryXHR) => this.reportError("Failed to get the setup nodes ips", response.responseText, response.statusText));            
    }
}

export = getSetupLocalNodeIpsCommand;
