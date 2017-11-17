import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getSetupLocalNodeIpsCommand extends commandBase {

    execute(): JQueryPromise<Array<string>> {      
        const url = endpoints.global.setup.setupIps;
        
        return this.query<Array<string>>(url, null, null, x => _.flatMap(x.NetworkInterfaces.map((i: any) => i.Addresses)))  
            .fail((response: JQueryXHR) => this.reportError("Failed to get the setup nodes ips", response.responseText, response.statusText));            
    }
}

export = getSetupLocalNodeIpsCommand;
