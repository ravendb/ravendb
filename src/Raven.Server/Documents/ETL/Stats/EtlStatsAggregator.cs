using System.Diagnostics;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlStatsAggregator : StatsAggregator<EtlRunStats, EtlStatsScope>
    {
        public EtlStatsAggregator(int id, EtlStatsAggregator lastStats) : base(id, lastStats)
        {
        }

        public override EtlStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);

            return Scope = new EtlStatsScope(Stats);
        }
    }
}