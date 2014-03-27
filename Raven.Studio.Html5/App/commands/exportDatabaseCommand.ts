import commandBase = require("commands/commandBase");
import alertType = require("common/alertType");
import database = require("models/database");

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