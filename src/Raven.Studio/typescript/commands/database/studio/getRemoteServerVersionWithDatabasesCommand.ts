import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getRemoteServerVersionWithDatabasesCommand extends commandBase {

    constructor(private serverUrl: string, private userName: string, private password: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames> {
        const args = {
            serverUrl: this.serverUrl,
            userName: this.userName,
            password: this.password
        };
        
        const url = endpoints.global.databases.adminRemoteServerBuildVersion + this.urlEncodeArgs(args);
        return this.query(url, null);
    }
}

export = getRemoteServerVersionWithDatabasesCommand; 
