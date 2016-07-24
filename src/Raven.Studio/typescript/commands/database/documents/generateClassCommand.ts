import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class generateClassCommand extends commandBase {
    
    constructor(private db: database, private docId: string, private lang: string) {
        super();
    }/*
    isGeneratingCode = ko.observable(true);

    execute(): JQueryPromise<string> {

        var url = "/generate/code";
        var args = {
            docId: this.docId,
            lang: this.lang
        }
        return this.query(url, args, this.db)
            .done((result) => {
                return result.Code;
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to create class code", response.responseText, response.statusText));

    }

    activateGenerateCode() {
        this.isGeneratingCode(true);
    }*/
}

export = generateClassCommand;
