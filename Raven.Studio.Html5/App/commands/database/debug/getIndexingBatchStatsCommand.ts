import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import d3 = require("d3/d3");
import appUrl = require("common/appUrl");

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
            var mappedResult = result.map(item => { return {
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

            mappedResult.forEach(r => {
                r.PerfStats.forEach(p => {
                    p.stats.Operations.filter(x => !("Name" in x)).forEach(o => {
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
