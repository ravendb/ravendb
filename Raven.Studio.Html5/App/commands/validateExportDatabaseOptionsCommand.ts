import commandBase = require("commands/commandBase");
import database = require("models/database");

class validateExportDatabaseOptionsCommand extends commandBase {

    constructor(private smugglerOptions: smugglerOptionsDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {

        return this.post("/studio-tasks/validateExportOptions", JSON.stringify(this.smugglerOptions), this.db);
    }
}

export = validateExportDatabaseOptionsCommand;
