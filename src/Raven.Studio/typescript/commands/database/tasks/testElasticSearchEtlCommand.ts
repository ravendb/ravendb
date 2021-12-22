import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testElasticSearchEtlCommand extends commandBase {
    constructor(private db: database, private payload: Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.TestElasticSearchEtlScript) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.ElasticSearchEtlTestScriptResult> {
        const url = endpoints.databases.elasticSearchEtl.adminEtlElasticsearchTest;

        return this.post<Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.ElasticSearchEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to test Elasticsearch ETL`, response.responseText, response.statusText);
            });
    }
}

export = testElasticSearchEtlCommand;
