import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class testElasticSearchNodeConnectionCommand extends commandBase {

    constructor(private serverUrl: string, private authenticationDto: Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            url: this.serverUrl
        };
        
        const url = endpoints.global.elasticSearchEtlConnection.adminEtlElasticsearchTestConnection + this.urlEncodeArgs(args);
        const payload = this.authenticationDto;

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(`Failed to test Elasticsearch connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test Elasticsearch connection`, result.Error);
                }
            });
    }
}

export = testElasticSearchNodeConnectionCommand;
