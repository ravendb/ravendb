import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import EtlErrors = Raven.Server.Documents.ETL.Stats.TaskErrors;

interface EtlErrorsArgs extends databaseLocationSpecifier {
    name?: string[];
}

class getEtlErrorsCommand extends commandBase {
    constructor(private db: database | string, private location: databaseLocationSpecifier, private taskNames: string[] = []) {
        super();
    }

    execute(): JQueryPromise<EtlErrors[]> {
        const args: EtlErrorsArgs = { ...this.location };

        if (this.taskNames.length > 0) {
            args.name = this.taskNames;
            const url = endpoints.databases.etl.etlErrors + this.urlEncodeArgs(args);
            return this.query<EtlErrors[]>(url, null, this.db, (res) => res.Results);
        }
        
        const url = endpoints.databases.taskErrors.tasksErrors + this.urlEncodeArgs(args);
        return this.query<EtlErrors[]>(url, null, this.db, (res) => res.Results);
    }
}

export default getEtlErrorsCommand
