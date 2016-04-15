import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class deleteDocumentCommand extends commandBase {

    constructor(private docId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {        
        return this.del('/docs?id=' + this.docId, null, this.db);
    }
}

export = deleteDocumentCommand;
