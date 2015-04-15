import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class exportDatabaseCommand extends commandBase {

    constructor(private smugglerOptions: smugglerOptionsDto, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {

      return this.post("/studio-tasks/exportDatabase", this.smugglerOptions, this.db)
        .fail((response: JQueryXHR) => {
          debugger
        })
        .done((res, fs, g) => {
          debugger
        });
    }
}

export = exportDatabaseCommand;