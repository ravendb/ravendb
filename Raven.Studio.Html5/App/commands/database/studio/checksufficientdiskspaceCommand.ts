import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class checkSufficientDiskSpaceCommand extends commandBase {

    constructor(private fileSize: number, private database: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var args = {
            fileSize: this.fileSize
        }
        var url = "/studio-tasks/check-sufficient-diskspace";
        var checkTask = this.query(url, args, this.database, undefined, { dataType: undefined });

        return checkTask;
    }
}

export = checkSufficientDiskSpaceCommand; 
