import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import document = require("models/database/documents/document");

class listDiskPerformanceRunsCommand extends commandBase {
    private db: database = appUrl.getDatabase();

    constructor() {
        super();
    }

    execute(): JQueryPromise<performanceRunItemDto[]> {
        var args = {
            fetch: "DisplayName"
        };

        var mapper = (x: any) => {
            return {
                displayName: x.DisplayName,
                documentId: x["__document_id"]
            }
        }

        return this.query("/indexes/dynamic/DiskIoPerformanceRuns", args, this.db, r => r.Results.map(mapper));
    }
}

export = listDiskPerformanceRunsCommand;
