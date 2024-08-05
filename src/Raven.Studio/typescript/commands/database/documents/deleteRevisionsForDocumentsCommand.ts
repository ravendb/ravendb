import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
type Parameters = Raven.Client.Documents.Operations.Revisions.DeleteRevisionsOperation.Parameters;

class deleteRevisionsForDocumentsCommand extends commandBase {

    private readonly databaseName: string;
    private readonly parameters: Parameters;

    constructor(databaseName: string, parameters: Parameters) {
        super();
        this.databaseName = databaseName;
        this.parameters = parameters;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.adminRevisions.adminRevisions;

        return this.del<void>(url, JSON.stringify(this.parameters), this.databaseName, { dataType: undefined });
    }
 }

export = deleteRevisionsForDocumentsCommand;
