import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getRemoteServerVersionWithDatabasesCommand extends commandBase {

    constructor(private serverUrl: string,
        private userName: string, private password: string, private domain: string,
        private apiKey: string, private enableBasicAuthenticationOverUnsecuredHttp: boolean,
        private skipServerCertificateValidation: boolean) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames> {
        const args = {
            serverUrl: this.serverUrl,
            userName: this.userName,
            password: this.password,
            domain: this.domain,
            apiKey: this.apiKey,
            enableBasicAuthenticationOverUnsecuredHttp: this.enableBasicAuthenticationOverUnsecuredHttp,
            skipServerCertificateValidation: this.skipServerCertificateValidation
        };
        
        const url = endpoints.global.databases.adminRemoteServerBuildVersion + this.urlEncodeArgs(args);
        return this.query(url, null);
    }
}

export = getRemoteServerVersionWithDatabasesCommand; 
