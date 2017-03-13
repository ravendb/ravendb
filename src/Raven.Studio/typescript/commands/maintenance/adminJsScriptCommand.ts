import commandBase = require("commands/commandBase");

class adminJsScriptCommand extends commandBase {
    constructor(private script: string, private targetDatabase:string) {
        super();
    }

    execute(): JQueryPromise<any> {
        
        var url = "/admin/console/" + this.targetDatabase.toString();//TODO: use endpoints
        return this.post(url, ko.toJSON({ script: this.script }), null)
            .done(() => this.reportSuccess("Script executed"))
            .fail((response: JQueryXHR) => this.reportError("Script failed", response.responseText, response.statusText));

    }
}

export = adminJsScriptCommand;
