import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

interface RemoteServerBuildInfoArgs {
    serverUrl: string;
    userName: string;
    password: string;
    domain: string;
    apiKey: string;
    enableBasicAuthenticationOverUnsecuredHttp: boolean;
    skipServerCertificateValidation: boolean;
}

class getRemoteServerVersionWithDatabasesCommand extends commandBase {

    constructor(private args: RemoteServerBuildInfoArgs) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames> {
        const url = endpoints.global.databases.adminRemoteServerBuildVersion + this.urlEncodeArgs(this.args);
        return this.query(url, null);
    }
}

export = getRemoteServerVersionWithDatabasesCommand; 
