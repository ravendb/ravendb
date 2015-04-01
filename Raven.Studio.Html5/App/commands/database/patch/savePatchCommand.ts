import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import patchDocument = require("models/database/patch/patchDocument");
import saveDocumentCommand = require('commands/database/documents/saveDocumentCommand');

class savePatchCommand extends commandBase {

    constructor(private patchName: string, private patchDocument: patchDocument, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Patch.");
        return new saveDocumentCommand("Studio/Patch/" + this.patchName, this.patchDocument, this.db, false).execute()
            .done(() => this.reportSuccess("Saved Patch."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Patch.", response.responseText, response.statusText));
    }
        
}
export = savePatchCommand;