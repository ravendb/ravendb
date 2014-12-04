import commandBase = require("commands/commandBase");
import database = require("models/database");
import d3 = require("d3/d3");

class getIndexingBatchStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<indexingBatchInfoDto[]> {
        var url = "/debug/indexing-batch-stats";
        var entryMapping = (entry) => {
            return {
                indexName: entry.key,
                stats: entry.value
            }
        };

        var parser = d3.time.format.iso;

        return this.query<indexingBatchInfoDto[]>(url, null, this.db, result => {
            return result.map(item => { return {
                BatchType: item.BatchType,
                IndexesToWorkOn: item.IndexesToWorkOn,
                TotalDocumentCount: item.TotalDocumentCount,
                TotalDocumentSize: item.TotalDocumentSize,
                StartedAt: item.StartedAt,
                StartedAtDate: parser.parse(item.StartedAt),
                TotalDurationMs: item.TotalDurationMs,
                PerfStats: d3.map(item.PerformanceStats).entries().map(entryMapping)
            }
            });
        });
    }
}

export = getIndexingBatchStatsCommand;
