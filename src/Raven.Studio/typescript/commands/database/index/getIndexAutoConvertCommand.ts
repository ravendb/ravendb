import commandBase from "commands/commandBase";
import endpoints = require("endpoints");

export interface IndexAutoConvertArgs {
    name: string;
    outputType: Raven.Server.Documents.Handlers.Processors.Indexes.ConversionOutputType;
    download?: boolean;
}

export interface IndexAutoConvertToJsonResultsDto {
    Indexes: Raven.Client.Documents.Indexes.IndexDefinition[];
}

export class getIndexAutoConvertCommand extends commandBase {
    private readonly databaseName: string;
    private readonly urlArgs: IndexAutoConvertArgs;

    constructor(databaseName: string, urlArgs: IndexAutoConvertArgs) {
        super();
        this.databaseName = databaseName;
        this.urlArgs = urlArgs;
    }

    execute(): JQueryPromise<string> {
        const url = endpoints.databases.index.indexesAutoConvert + this.urlEncodeArgs(this.urlArgs);

        return this.query<string>(url, null, this.databaseName, null, { dataType: "text" }).fail(
            (response: JQueryXHR) => {
                this.reportError("Failed to convert index", response.responseText, response.statusText);
            }
        );
    }
}
