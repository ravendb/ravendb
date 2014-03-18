import commandBase = require("commands/commandBase");
import database = require("models/database");

class saveCsvFileCommand extends commandBase {

    constructor(private file, private fileName, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var customHeaders = {
            'X-FileName': this.fileName
        };

        var jQueryOptions: JQueryAjaxSettings = {
            headers: <any>customHeaders,
            dataType: 'text' 
        };
        var createSampleDataTask = this.post("/studio-tasks/loadCsvFile", { file: this.file}, this.db, jQueryOptions);
        return createSampleDataTask;
}
}

export = saveCsvFileCommand;