import commandBase = require("commands/commandBase");
import alertType = require("common/alertType");
import database = require("models/database");

class importDatabaseCommand extends commandBase {

    constructor(private collectionName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Importing...");

        var url = "/studio-tasks/import";
        var deleteTask = this.del(url, null, this.db);
        deleteTask.done(() => this.reportSuccess("Imported"));
        deleteTask.fail((response) => this.reportError("Failed to import", JSON.stringify(response)));
        return deleteTask;
    }
}

export = importDatabaseCommand; 