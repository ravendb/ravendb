using System.Linq;
using Raven.Server.Utils;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlStatsScope : StatsScope<EtlRunStats, EtlStatsScope>
    {
        private readonly EtlRunStats _stats;

        public EtlStatsScope(EtlRunStats stats, bool start = true) : base(stats, start)
        {
            _stats = stats;
        }

        protected override EtlStatsScope OpenNewScope(EtlRunStats stats, bool start)
        {
            return new EtlStatsScope(stats, start);
        }

        public int NumberOfExtractedItems => _stats.NumberOfExtractedItems;

        public int NumberOfTransformedItems => _stats.NumberOfTransformedItems;

        public long LastTransformedEtag => _stats.LastTransformedEtag;

        public long LastLoadedEtag => _stats.LastLoadedEtag;

        public long LastFilteredOutEtag => _stats.LastFilteredOutEtag;

        public string ChangeVector => _stats.ChangeVector;

        public string BatchCompleteReason => _stats.BatchCompleteReason;

        public void RecordExtractedItem()
        {
            _stats.NumberOfExtractedItems++;
        }

        public void RecordTransformedItem()
        {
            _stats.NumberOfTransformedItems++;
        }

        public void RecordLastTransformedEtag(long etag)
        {
            _stats.LastTransformedEtag = etag;
        }

        public void RecordChangeVector(string changeVector)
        {
            _stats.ChangeVector = ChangeVectorUtils.MergeVectors(_stats.ChangeVector, changeVector);
        }

        public void RecordLastLoadedEtag(long etag)
        {
            _stats.LastLoadedEtag = etag;
        }

        public void RecordLastFilteredOutEtag(long etag)
        {
            _stats.LastFilteredOutEtag = etag;
        }

        public EtlPerformanceOperation ToPerformanceOperation(string name)
        {
            var operation = new EtlPerformanceOperation(Duration)
            {
                Name = name
            };

            if (Scopes != null)
            {
                operation.Operations = Scopes
                    .Select(x => x.Value.ToPerformanceOperation(x.Key))
                    .ToArray();
            }

            return operation;
        }

        public void RecordBatchCompleteReason(string reason)
        {
            _stats.BatchCompleteReason = reason;
        }
    }
}
