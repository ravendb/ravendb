import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class deleteDocsMatchingQueryCommand extends commandBase {
    constructor(private indexName: string, private queryText: string, private defaultOperator: string, private db: database) {
        super(); 
    } 
       
    execute(): JQueryPromise<{ OperationId: number; }> {
        this.reportInfo("Deleting docs matching query..."); 
         
        var args = {
            query: this.queryText, 
            pageSize: 128, 
            allowStale: false,
            operator: this.defaultOperator
        };

        var url = "/bulk_docs/" + this.indexName + this.urlEncodeArgs(args);
        var task = this.del(url, null, this.db);
        task.done(() => this.reportSuccess("Docs deleted"));
        task.fail((response: JQueryXHR) => this.reportError("Error deleting docs matching query", response.responseText, response.statusText));

        return task;
    }

}

export = deleteDocsMatchingQueryCommand; 
