import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexFieldsFromMapCommand extends commandBase {

    constructor(private db: database,
                private map: string,
                private additionalSources: dictionary<string>,
                private additionalAssemblies: Array<Raven.Client.Documents.Indexes.AdditionalAssembly>) {
        super();
    }

    execute(): JQueryPromise<resultsDto<string>> {
        const url = endpoints.databases.studioIndex.studioIndexFields;
        
        const args = {
            Map: this.map,
            AdditionalSources: this.additionalSources,
            AdditionalAssemblies: this.additionalAssemblies
        };
        
        return this.post(url, JSON.stringify(args), this.db);
    }
} 

export = getIndexFieldsFromMapCommand;
