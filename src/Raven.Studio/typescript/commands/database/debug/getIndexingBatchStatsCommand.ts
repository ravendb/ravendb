import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import d3 = require("d3");
import appUrl = require("common/appUrl");

class getIndexingBatchStatsCommand extends commandBase {

    constructor(private db: database, private lastId: number = 0) {
        super();
    }

    execute(): JQueryPromise<indexingBatchInfoDto[]> {
        var url = "/debug/indexing-batch-stats";
        var entryMapping = (entry: { key: string, value: any}) => {
            return {
                indexName: entry.key,
                stats: entry.value
            }
        };
        var args = { lastId: this.lastId };

        var parser = d3.time.format.iso;

        return this.query<indexingBatchInfoDto[]>(url, args, this.db, result => {
            var mappedResult: indexingBatchInfoDto[] = result.map((item: indexingBatchInfoDto) => {
                return {
                    Id: item.Id,
                    BatchType: item.BatchType,
                    IndexesToWorkOn: item.IndexesToWorkOn,
                    TotalDocumentCount: item.TotalDocumentCount,
                    TotalDocumentSize: item.TotalDocumentSize,
                    StartedAt: item.StartedAt,
                    StartedAtDate: parser.parse(item.StartedAt),
                    TotalDurationMs: item.TotalDurationMs,
                    PerfStats: d3.map((<any>item).PerformanceStats).entries().map(entryMapping)
                }
            });

            mappedResult.forEach(r => {
                r.PerfStats.forEach(p => {
                    p.stats.Operations.filter(x => !("Name" in x)).forEach((o: parallelPefromanceStatsDto) => {
                        o.BatchedOperations.forEach(b => {
                            b.Operations.forEach(x => { 
                                x.ParallelParent = b;
                            });
                            b.Parent = o;
                        });
                    });
                });
            });

            return mappedResult;
        });
    }

    getQueryUrl(): string {
        return appUrl.forResourceQuery(this.db) + this.getQueryUrlFragment();
    }

    private getQueryUrlFragment(): string {
        return "/debug/indexing-perf-stats-with-timings";
    }
}

export = getIndexingBatchStatsCommand;
