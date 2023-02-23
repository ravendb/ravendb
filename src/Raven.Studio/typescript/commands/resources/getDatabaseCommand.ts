import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

/**
 * @deprecated please use getDatabaseForStudioCommand or getDatabasesStateForStudioCommand
 */
class getDatabaseCommand extends commandBase {

    constructor(private dbName: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        const url = endpoints.global.databases.databases;
        const args = {
            name: this.dbName
        };

        return this.query<any>(url, args, null, x => x.Databases[0]);
    }
}

export = getDatabaseCommand;
