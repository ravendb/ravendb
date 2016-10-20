import commandBase = require("commands/commandBase");

class listDiskPerformanceRunsCommand extends commandBase {

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

        return this.query("/indexes/dynamic/DiskIoPerformanceRuns", args, null, (r: any) => r.Results.map(mapper));//TODO: use endpoints
    }
}

export = listDiskPerformanceRunsCommand;
