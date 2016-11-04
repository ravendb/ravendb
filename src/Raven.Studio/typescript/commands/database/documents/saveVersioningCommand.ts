import database = require("models/resources/database");
import document = require("models/database/documents/document");
import commandBase = require("commands/commandBase");

//TODO: we probably won't need this class
class saveVersioningCommand extends commandBase {
    constructor(private db: database, private versioningConfiguration: Raven.Server.Documents.Versioning.VersioningConfiguration) {
        super();
    }

    /* TODO:

    execute(): JQueryPromise<any> {

        var commands: bulkDocumentDto[] = [];

        this.versioningEntries.forEach((dto: versioningEntryDto) => {
            var entry: any = new document(dto);
            commands.push({
                Key: (this.globalConfig ? "Raven/Global/Versioning/": "Raven/Versioning/") + entry["Id"],
                Method: "PUT",
                Document: entry.toDto(false),
                Metadata: entry.__metadata.toDto(),
                Etag: entry.__metadata.etag
            });
        });

        var saveTask = new saveBulkOfDocuments("versioning", commands, this.db).execute();
        return saveTask;
    }*/
}

export = saveVersioningCommand;
