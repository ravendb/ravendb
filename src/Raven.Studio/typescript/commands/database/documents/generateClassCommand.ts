import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class generateClassCommand extends commandBase {
    
    constructor(private db: database, private docId: string, private lang: string) {
        super();
    }

    execute(): JQueryPromise<string> {
        const url = endpoints.databases.document.docsClass;
        const args = {
            id: this.docId,
            lang: this.lang
        }
        return this.query(url, args, this.db, null, { dataType: "text" })
            .fail((response: JQueryXHR) => this.reportError("Failed to create class code",
                response.responseText,
                response.statusText));
    }
}

export = generateClassCommand;
