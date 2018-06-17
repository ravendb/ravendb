import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDocumentPhysicalSizeCommand extends commandBase {

    constructor(private id: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {

        const args = {
            id: this.id
        };
        
        const url = endpoints.databases.document.docsSize + this.urlEncodeArgs(args);

        return this.query<any>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get the document physical size", response.responseText, response.statusText));
    }
}
 
export = getDocumentPhysicalSizeCommand;
