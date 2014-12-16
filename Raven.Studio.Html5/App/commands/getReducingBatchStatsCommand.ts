import commandBase = require("commands/commandBase");
import database = require("models/database");
import d3 = require("d3/d3");

class getReducingBatchStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<reducingBatchInfoDto[]> {
        var url = "/debug/reducing-batch-stats";
        var entryMapping: (any) => indexNameAndReducingPerformanceStats = (entry) => {
            return {
                indexName: entry.key,
                stats: entry.value
            }
        };

        var parser = d3.time.format.iso;

        return this.query<reducingBatchInfoDto[]>(url, null, this.db, result => {
            var mappedResult: reducingBatchInfoDto[] = result.map(item => { return {
                IndexesToWorkOn: item.IndexesToWorkOn,
                TotalDurationMs: item.TotalDurationMs,
                StartedAt: item.StartedAt,
                StartedAtDate: parser.parse(item.StartedAt),
                PerfStats: d3.map(item.PerformanceStats).entries().map(entryMapping),
                TimeSinceFirstReduceInBatchCompletedMs: item.TimeSinceFirstReduceInBatchCompletedMs
            }
            });

            mappedResult.forEach(r => {
                r.PerfStats.forEach(s => {
                    s.stats.LevelStats.forEach(l => {
                        l.parent = s;
                    });
                    s.parent = r;
                });
            });

            return mappedResult;
        });
    }
}

export = getReducingBatchStatsCommand;
