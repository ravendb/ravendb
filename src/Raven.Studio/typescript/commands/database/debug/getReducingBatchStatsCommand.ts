import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import d3 = require("d3");

class getReducingBatchStatsCommand extends commandBase {

    constructor(private db: database, private lastId: number) {
        super();
    }

    execute(): JQueryPromise<reducingBatchInfoDto[]> {
        var url = "/debug/reducing-batch-stats";//TODO: use endpoints
        var args = { lastId: this.lastId };
        var inlinePerfStats = (entry: any[]) => {
            var result: { indexName: string, stats: any[] }[] = [];
            d3.map<any[]>(entry).entries().forEach(e => {
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

        return this.query<reducingBatchInfoDto[]>(url, args, this.db, result => {
            var mappedResult: reducingBatchInfoDto[] = result.map((item: reducingBatchInfoDto) => {
                return {
                Id: item.Id,
                IndexesToWorkOn: item.IndexesToWorkOn,
                TotalDurationMs: item.TotalDurationMs,
                StartedAt: item.StartedAt,
                StartedAtDate: parser.parse(item.StartedAt),
                PerfStats: inlinePerfStats((<any>item).PerformanceStats),
                TimeSinceFirstReduceInBatchCompletedMs: item.TimeSinceFirstReduceInBatchCompletedMs
            }
            });

            mappedResult.forEach(r => {
                r.PerfStats.forEach(s => {
                    s.stats.LevelStats.forEach(l => {
                        l.Operations.filter(x => !("Name" in x)).forEach((o: parallelPefromanceStatsDto) => {
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
