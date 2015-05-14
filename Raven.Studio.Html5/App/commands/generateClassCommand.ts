import commandBase = require("commands/commandBase");
import database = require("models/database");

class generateClassCommand extends commandBase {

    constructor(private db: database, private docId: string, private lang: string) {
        super();
    }
    isGeneratingCode = ko.observable(true);

    execute(): JQueryPromise<any> {

        var url = "/generate/code";
        var args = {
            docId: this.docId,
            lang: this.lang
        }
        return this.query(url, args,this.db);
    }

    activateGenerateCode() {
        this.isGeneratingCode(true);
    }
}

export = generateClassCommand;