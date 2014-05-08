import commandBase = require("commands/commandBase");
import alertType = require("common/alertType");
import database = require("models/database");

class importDatabaseCommand extends commandBase {

    constructor(private fileData: FormData, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Importing...");

        var url = "/studio-tasks/import";
        var ajaxOptions: JQueryAjaxSettings = {
            processData: false, // Prevents JQuery from automatically transforming the data into a query string. http://api.jquery.com/jQuery.ajax/
            contentType: false,
            dataType: "text" // The server sends back an empty string, which is invalid JSON. So we must specify the return type as plain text, rather than JSON.
        }
        var importTask = this.post(url, this.fileData, this.db, ajaxOptions);
        importTask.done(() => this.reportSuccess("Database imported"));
        importTask.fail((response: JQueryXHR) => this.reportError("Failed to import database", response.responseText, response.statusText));
        return importTask;
    }
}

export = importDatabaseCommand; 