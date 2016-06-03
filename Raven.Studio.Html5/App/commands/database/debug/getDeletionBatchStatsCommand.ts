import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import d3 = require("d3/d3");

class getDeletionBatchStatsCommand extends commandBase {

    constructor(private db: database, private lastId: number) {
        super();
    }

    execute(): JQueryPromise<deletionBatchInfoDto[]> {
        var url = "/debug/deletion-batch-stats";
        var args = { lastId: this.lastId };
        var parser = d3.time.format.iso;

        return this.query<deletionBatchInfoDto[]>(url, args, this.db, result => {
            result.forEach(item => {
                item.StartedAtDate = parser.parse(item.StartedAt);
            });
            return result;
        });
    }
}

export = getDeletionBatchStatsCommand;
