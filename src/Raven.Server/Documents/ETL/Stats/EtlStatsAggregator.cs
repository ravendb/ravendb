using System;
using System.Diagnostics;
using Raven.Server.Utils.Stats;
using Sparrow;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents.ETL.Stats
{
    public interface IEtlStatsAggregator : IStatsAggregator
    {
        EtlPerformanceStats ToPerformanceStats();

        EtlPerformanceStats ToPerformanceLiveStatsWithDetails();

        EtlPerformanceStats ToPerformanceLiveStats();

        bool Completed { get; }
    }

    public class EtlStatsAggregator<TStatsScope, TEtlPerformanceOperation> : StatsAggregator<EtlRunStats, TStatsScope>, IEtlStatsAggregator
        where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
        where TEtlPerformanceOperation : EtlPerformanceOperation
    {
        private readonly Func<EtlRunStats, TStatsScope> _factory;
        private volatile EtlPerformanceStats _performanceStats;

        public EtlStatsAggregator(int id, Func<EtlRunStats, TStatsScope> factory, IEtlStatsAggregator lastStats) : base(id, lastStats)
        {
            _factory = factory;
        }

        public override TStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);

            return Scope = _factory(Stats);
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
                BatchCompleteReason = Stats.BatchCompleteReason,
                CurrentlyAllocated = new Size(Stats.CurrentlyAllocated.GetValue(SizeUnit.Bytes)),
                BatchSize = new Size(Stats.BatchSize.GetValue(SizeUnit.Bytes)),
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

        public IStatsScope EtlScope => Scope;
    }
}
