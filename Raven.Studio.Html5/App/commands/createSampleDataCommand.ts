import commandBase = require("commands/commandBase");

class createSampleDataComman extends commandBase
{
    constructor(private db: database) {
        super();
    }
    execute(): JQueryPromise<any> {
        this.reportInfo("Creating Sample Data, Please wait...");

        var saveBulkTask = this.post("/bulk_docs", null, this.db);
        return null;
    }

}

export = createSampleDataComman;