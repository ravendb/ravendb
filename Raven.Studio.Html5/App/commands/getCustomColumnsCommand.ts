import commandBase = require("commands/commandBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");

class getCustomColumnsCommand extends getDocumentWithMetadataCommand {

    static docsPrefix = "Raven/Studio/Columns/";

    constructor(db: database, docName: string) {
        super(getCustomColumnsCommand.docsPrefix + docName, db, true);
    }

    execute(): JQueryPromise<alertContainerDto> {
        return super.execute();
    }

    static forIndex(indexName: string, db: database): getCustomColumnsCommand {
        return new getCustomColumnsCommand(db, "Index/" + indexName);
    }

    static forCollection(collection: string, db: database): getCustomColumnsCommand {
        return new getCustomColumnsCommand(db, "Collection/" + collection);
    }

    static forAllDocuments(db: database) :getCustomColumnsCommand {
        return new getCustomColumnsCommand(db, "AllDocuments");
    }


}

export = getCustomColumnsCommand;