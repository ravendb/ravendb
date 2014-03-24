import commandBase = require("commands/commandBase");
import alertType = require("common/alertType");
import database = require("models/database");

class exportDatabaseCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {

      return this.post("/studio-tasks/exportDatabase", null, this.db)
        .fail((response: JQueryXHR) => {
          debugger
        })
        .done((res, fs, g) => {
          debugger
        });
    }
}

export = exportDatabaseCommand;