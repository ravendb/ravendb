import database = require("models/database");
import document = require("models/document");
import commandBase = require("commands/commandBase");
import saveBulkOfDocuments = require("commands/saveBulkOfDocuments");

class saveVersioningCommand extends commandBase {
    constructor(private db: database, private versioningEntries: Array<versioningEntryDto>, private removeEntries: Array<versioningEntryDto> = [], private globalConfig = false) {
        super();
    }


    execute(): JQueryPromise<any> {
        var commands: bulkDocumentDto[] = [];

        this.versioningEntries.forEach((dto: versioningEntryDto) => {
            var entry: document = new document(dto);
            commands.push({
                Key: (this.globalConfig ? "Raven/Global/Versioning/": "Raven/Versioning/") + entry["Id"],
                Method: "PUT",
                Document: entry.toDto(false),
                Metadata: entry.__metadata.toDto(),
                Etag: entry.__metadata.etag
            });
        });

        this.removeEntries.forEach((dto: versioningEntryDto) => {
            var entry: document = new document(dto);
            commands.push({
                Key: (this.globalConfig ? "Raven/Global/Versioning/" : "Raven/Versioning/") + entry["Id"],
                Method: "DELETE",
                Etag: entry.__metadata.etag
            });
        });

        var saveTask = new saveBulkOfDocuments("versioning", commands, this.db).execute();
        return saveTask;
    }
}

export = saveVersioningCommand;