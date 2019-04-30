import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDocumentIDFromChangeVectorCommand extends commandBase {
    
    constructor(private db: database, private changeVector: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.DocumentIDDetails> {      
        const args = {
            changeVector: this.changeVector
        };

        const url = endpoints.databases.document.docsId + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Documents.Handlers.DocumentIDDetails>(url, null, this.db);
    }
}

export = getDocumentIDFromChangeVectorCommand;
