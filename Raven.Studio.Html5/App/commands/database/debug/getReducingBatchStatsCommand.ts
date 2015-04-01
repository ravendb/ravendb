import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import d3 = require("d3/d3");

class getReducingBatchStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<reducingBatchInfoDto[]> {
        var url = "/debug/reducing-batch-stats";
        var inlinePerfStats = (entry) => {
            var result = [];
            d3.map(entry).entries().forEach(e => {
                e.value.forEach(s => {
                    result.push({
                        indexName: e.key,
                        stats: s
                    });
                });
            });
            return result;
        }

        var parser = d3.time.format.iso;

        return this.query<reducingBatchInfoDto[]>(url, null, this.db, result => {
            var mappedResult: any = result.map(item => { return {
                IndexesToWorkOn: item.IndexesToWorkOn,
                TotalDurationMs: item.TotalDurationMs,
                StartedAt: item.StartedAt,
                StartedAtDate: parser.parse(item.StartedAt),
                PerfStats: inlinePerfStats(item.PerformanceStats),
                TimeSinceFirstReduceInBatchCompletedMs: item.TimeSinceFirstReduceInBatchCompletedMs
            }
            });

            mappedResult.forEach(r => {
                r.PerfStats.forEach(s => {
                    s.stats.LevelStats.forEach(l => {
                        l.Operations.filter(x => !("Name" in x)).forEach(o => {
                            o.BatchedOperations.forEach(b => {
                                b.Operations.forEach(x => {
                                    x.ParallelParent = b;
                                });
                                b.Parent = o;
                            });
                        });
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
