import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class listDiskPerformanceRunsCommand extends commandBase {
    private db: database = appUrl.getDatabase();

    constructor() {
        super();
    }

    execute(): JQueryPromise<performanceRunItemDto[]> {
        var args: { fetch: string } = {
            fetch: "DisplayName"
        };

        var mapper = (x: any) => {
            return {
                displayName: x.DisplayName,
                documentId: x["__document_id"]
            };
        };

        return this.query("/indexes/dynamic/DiskIoPerformanceRuns", args, this.db, (r: any) => r.Results.map(mapper));
    }
}

export = listDiskPerformanceRunsCommand;
