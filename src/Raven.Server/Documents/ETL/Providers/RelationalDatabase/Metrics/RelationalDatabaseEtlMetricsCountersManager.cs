using System.Collections.Concurrent;
using System.Linq;
using Raven.Server.Documents.ETL.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Metrics;

public sealed class RelationalDatabaseEtlMetricsCountersManager: EtlMetricsCountersManager
{
    public ConcurrentDictionary<string, RelationalDatabaseEtlTableMetrics> TablesMetrics { get; set; }

    public RelationalDatabaseEtlMetricsCountersManager()
    {
        TablesMetrics = new ConcurrentDictionary<string, RelationalDatabaseEtlTableMetrics>();
    }

    public RelationalDatabaseEtlTableMetrics GetTableMetrics(string tableName)
    {
        return TablesMetrics.GetOrAdd(tableName, new RelationalDatabaseEtlTableMetrics(tableName));
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(TablesMetrics)] = TablesMetrics.ToDictionary(x => x.Key, x => x.Value.ToRelationalEtlTableMetricsDataDictionary());

        return json;
    }
}
