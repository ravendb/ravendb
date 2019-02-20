import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class testClusterNodeConnectionCommand extends commandBase {

    constructor(private serverUrl: string, private databaseName?: string, private bidirectional: boolean = true) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            url: this.serverUrl,
            database: this.databaseName,
            bidirectional: this.bidirectional
        };
        const url = endpoints.global.testConnection.adminTestConnection + this.urlEncodeArgs(args);

        return this.post(url, null)
            .fail((response: JQueryXHR) => this.reportError(`Failed to test connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test connection`, result.Error);
                }
            });
    }
}

export = testClusterNodeConnectionCommand;
