import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class importDatabaseCommand extends commandBase {

    constructor(private fileData: FormData, private batchSize: number, private includeExpiredDocuments: boolean, private stripReplicationInformation: boolean, private shouldDisableVersioningBundle: boolean, private operateOnTypes: ImportItemType[], private filters: filterSettingDto[], private transformScript: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Importing data...");

        var args = {
            batchSize: this.batchSize,
            includeExpiredDocuments: this.includeExpiredDocuments,
            stripReplicationInformation: this.stripReplicationInformation,
            shouldDisableVersioningBundle: this.shouldDisableVersioningBundle,
            operateOnTypes: this.operateOnTypes.reduce((first: ImportItemType, second: ImportItemType) => first | second, 0),
            filtersPipeDelimited: this.filters.filter(f => !!f.Path).map(f => f.Path + ";;;" + f.Values[0] + ";;;" + f.ShouldMatch).join("|||") || "",
            transformScript: this.transformScript || ""
        }

        var url = "/studio-tasks/import" + this.urlEncodeArgs(args);
        var ajaxOptions: JQueryAjaxSettings = {
            processData: false, // Prevents JQuery from automatically transforming the data into a query string. http://api.jquery.com/jQuery.ajax/
            contentType: false
        }
        var importTask = this.post(url, this.fileData, this.db, ajaxOptions); 
        importTask.done(() => this.reportInfo("Data was uploaded successfully, processing..."));
        importTask.fail((response: JQueryXHR) => this.reportError("Failed to upload data", response.responseText, response.statusText));
        return importTask;
    }
}

export = importDatabaseCommand; 
