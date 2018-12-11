using System.Collections.Concurrent;
using System.Linq;
using Raven.Server.Documents.ETL.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Metrics
{
    public class SqlEtlMetricsCountersManager : EtlMetricsCountersManager
    {
        public ConcurrentDictionary<string, SqlEtlTableMetrics> TablesMetrics { get; set; }

        public SqlEtlMetricsCountersManager()
        {
            TablesMetrics = new ConcurrentDictionary<string, SqlEtlTableMetrics>();
        }

        public SqlEtlTableMetrics GetTableMetrics(string tableName)
        {
            return TablesMetrics.GetOrAdd(tableName, name => new SqlEtlTableMetrics(name));
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(TablesMetrics)] = TablesMetrics.ToDictionary(x => x.Key, x => x.Value.ToSqlEtlTableMetricsDataDictionary());

            return json;
        }
    }
}
