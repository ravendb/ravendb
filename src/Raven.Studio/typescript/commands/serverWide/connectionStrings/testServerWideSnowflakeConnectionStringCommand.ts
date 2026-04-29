import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export default class testServerWideSnowflakeConnectionStringCommand extends commandBase {
    constructor(private readonly connectionString: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.snowflakeEtlServerWide.adminEtlSnowflakeTestConnection;

        return this.post(url, this.connectionString)
            .fail((response: JQueryXHR) => this.reportError(`Failed to test Snowflake connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test Snowflake connection`, result.Error);
                }
            });
    }
}
