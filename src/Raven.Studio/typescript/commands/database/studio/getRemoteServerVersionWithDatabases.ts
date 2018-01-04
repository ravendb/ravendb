import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getRemoteServerVersionWithDatabases extends commandBase {

    constructor(private serverUrl: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Smuggler.Migration.BuildInfoWithDatabaseNames> {
        const args = {
            serverUrl: this.serverUrl
        };
        
        const url = endpoints.global.databases.adminRemoteServerBuildVersion + this.urlEncodeArgs(args);
        
        return this.query(url, null);
            
    }
}

export = getRemoteServerVersionWithDatabases; 
