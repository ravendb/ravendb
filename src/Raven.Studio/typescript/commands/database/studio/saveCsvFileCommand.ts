import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class saveCsvFileCommand extends commandBase {

    constructor(private fileData: FormData, private fileName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Importing...");

        var customHeaders = {
            'X-FileName': this.fileName
        };

        var jQueryOptions: JQueryAjaxSettings = {
            headers: <any>customHeaders,
            processData: false,
            contentType: false,
            dataType: 'text' 
        };
        var saveTask = this.post("/studio-tasks/loadCsvFile", this.fileData, this.db, jQueryOptions);//TODO: use endpoints
        saveTask.done(() => this.reportSuccess("CSV file imported"));
        saveTask.fail((response: JQueryXHR) => this.reportError("Failed to import CSV file", response.responseText, response.statusText));
        return saveTask;

    }
}

export = saveCsvFileCommand;
