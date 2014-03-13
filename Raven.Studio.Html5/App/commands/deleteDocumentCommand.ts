import commandBase = require("commands/commandBase");
import database = require("models/database");

class deleteDocumentCommand extends commandBase {

    constructor(private docId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {        
        return this.del('/docs/' + this.docId, null, this.db);
    }
}

export = deleteDocumentCommand;