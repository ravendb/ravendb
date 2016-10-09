import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class checkSufficientDiskSpaceCommand extends commandBase {

    constructor(private fileSize: number, private database: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            fileSize: this.fileSize
        }
        const url = "/studio-tasks/check-sufficient-diskspace";//TODO: use endpoints
        return this.query<void>(url, args, this.database, undefined, { dataType: undefined });
    }
}

export = checkSufficientDiskSpaceCommand; 
