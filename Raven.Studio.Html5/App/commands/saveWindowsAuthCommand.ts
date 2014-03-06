import commandBase = require("commands/commandBase");
import document = require("models/document");
import database = require("models/database");

class saveWindowsAuthCommand extends commandBase {

    constructor(private dto: windowsAuthDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Windows Authentication settings.");
        return this.saveSetup()
            .done(() => this.reportSuccess("Saved Windows Authentication settings."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Windows Authentication settings.", response.responseText));
    }

    private saveSetup(): JQueryPromise<any> {
        var id = "Raven/Authorization/WindowsSettings";
        var url = "/docs/" + id;
        var putArgs = JSON.stringify(this.dto);
        return this.put(url, putArgs, null);
    }
}

export = saveWindowsAuthCommand;