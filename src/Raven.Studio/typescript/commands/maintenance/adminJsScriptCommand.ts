import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class adminJsScriptCommand extends commandBase {
    
    private readonly script: string;
    private readonly targetDatabase: string;
    
    constructor(script: string, targetDatabase?: string) {
        super();
        this.script = script;
        this.targetDatabase = targetDatabase;
    }

    execute(): JQueryPromise<{Result: any}> {
        const args = {
            serverScript: !this.targetDatabase,
            database: this.targetDatabase
        };

        const payload: Raven.Server.Documents.Patch.AdminJsScript = {
            Script: this.script
        };

        const url = endpoints.global.adminDatabases.adminConsole + this.urlEncodeArgs(args);
        return this.post(url, JSON.stringify(payload))
            .done(() => this.reportSuccess("Script executed"))
            .fail((response: JQueryXHR) => this.reportError("Script failed", response.responseText, response.statusText));
    }
}

export = adminJsScriptCommand;
