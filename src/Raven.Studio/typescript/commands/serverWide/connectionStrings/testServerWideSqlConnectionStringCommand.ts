import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export default class testServerWideSqlConnectionStringCommand extends commandBase {
    constructor(private readonly connectionString: string, private readonly factoryName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            factoryName: this.factoryName,
        };
        const url = endpoints.global.sqlEtlServerWide.adminEtlSqlTestConnection + this.urlEncodeArgs(args);

        return this.post(url, this.connectionString)
            .fail((response: JQueryXHR) => this.reportError(`Failed to test SQL connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test SQL connection`, result.Error);
                }
            });
    }
}
