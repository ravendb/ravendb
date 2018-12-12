using System;
using System.Diagnostics;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlStatsAggregator : StatsAggregator<EtlRunStats, EtlStatsScope>
    {
        private volatile EtlPerformanceStats _performanceStats;

        public EtlStatsAggregator(int id, EtlStatsAggregator lastStats) : base(id, lastStats)
        {
        }

        public override EtlStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);

            return Scope = new EtlStatsScope(Stats);
        }

        public EtlPerformanceStats ToPerformanceStats()
        {
            if (_performanceStats != null)
                return _performanceStats;

            lock (Stats)
            {
                if (_performanceStats != null)
                    return _performanceStats;

                return _performanceStats = CreatePerformanceStats(completed: true);
            }
        }

        private EtlPerformanceStats CreatePerformanceStats(bool completed)
        {
            return new EtlPerformanceStats(Scope.Duration)
            {
                Id = Id,
                Started = StartTime,
                Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
                Details = Scope.ToPerformanceOperation("ETL"),
                LastLoadedEtag = Stats.LastLoadedEtag,
                NumberOfLoadedItems = Stats.NumberOfLoadedItems,
                LastExtractedEtags = Stats.LastExtractedEtags,
                LastTransformedEtags = Stats.LastTransformedEtags,
                LastFilteredOutEtags = Stats.LastFilteredOutEtags,
                NumberOfExtractedItems = Stats.NumberOfExtractedItems,
                NumberOfTransformedItems = Stats.NumberOfTransformedItems,
                NumberOfTransformedTombstones = Stats.NumberOfTransformedTombstones,
                TransformationErrorCount = Scope.TransformationErrorCount,
                SuccessfullyLoaded = Stats.SuccessfullyLoaded,
                BatchCompleteReason = Stats.BatchCompleteReason
            };
        }

        public EtlPerformanceStats ToPerformanceLiveStatsWithDetails()
        {
            if (_performanceStats != null)
                return _performanceStats;

            if (Scope == null || Stats == null)
                return null;

            if (Completed)
                return ToPerformanceStats();

            return CreatePerformanceStats(completed: false);
        }

        public EtlPerformanceStats ToPerformanceLiveStats()
        {
            if (_performanceStats != null)
                return _performanceStats;

            if (Scope == null || Stats == null)
                return null;

            return new EtlPerformanceStats(Scope.Duration)
            {
                Started = StartTime,
                Completed = Completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
                LastLoadedEtag = Stats.LastLoadedEtag,
                LastTransformedEtags = Stats.LastTransformedEtags,
                LastFilteredOutEtags = Stats.LastFilteredOutEtags,
                NumberOfExtractedItems = Stats.NumberOfExtractedItems,
                NumberOfTransformedItems = Stats.NumberOfTransformedItems,
                NumberOfTransformedTombstones = Stats.NumberOfTransformedTombstones,
                TransformationErrorCount = Scope.TransformationErrorCount,
                SuccessfullyLoaded = Stats.SuccessfullyLoaded,
                BatchCompleteReason = Stats.BatchCompleteReason
            };
        }
    }
}
