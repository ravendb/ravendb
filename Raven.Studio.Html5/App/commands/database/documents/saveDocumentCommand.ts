import commandBase = require("commands/commandBase");
import document = require("models/database/documents/document");
import database = require("models/resources/database");

class saveDocumentCommand extends commandBase {

    constructor(private id: string, private document: document, private db: database, private reportSaveProgress = true) {
        super();
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        if (this.reportSaveProgress) {
            this.reportInfo("Saving " + this.id + "...");
        }

        var customHeaders = {
            'Raven-Client-Version': commandBase.ravenClientVersion,
        };

        var metadataDto: documentMetadataDto = this.document.__metadata.toDto();

        for (var key in metadataDto) {
            if (key.indexOf('@') !== 0) {
                customHeaders[key] = metadataDto[key];
            }
        }
        
        var jQueryOptions: JQueryAjaxSettings = {
            headers: <any>customHeaders
        };

        var documentDto: documentDto = this.document.toDto(false);

        var commands: Array<bulkDocumentDto> = [];
        commands.push({
            Method: "PUT",
            Key: this.id,
            Document: documentDto,
            Metadata: metadataDto
        });

        var args = ko.toJSON(commands);
        var url = "/bulk_docs";
        var saveTask = this.post(url, args, this.db, jQueryOptions);

        if (this.reportSaveProgress) {
            saveTask.done(() => this.reportSuccess("Saved " + this.id));
            saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save " + this.id, response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = saveDocumentCommand;