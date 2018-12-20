using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Util;
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

        public Dictionary<EtlItemType, int> NumberOfExtractedItems => _stats.NumberOfExtractedItems;

        public Dictionary<EtlItemType, int> NumberOfTransformedItems => _stats.NumberOfTransformedItems;

        public Dictionary<EtlItemType, long> LastTransformedEtags => _stats.LastTransformedEtags;

        public Dictionary<EtlItemType, long> LastFilteredOutEtags => _stats.LastFilteredOutEtags;

        public Dictionary<EtlItemType, long> LastExtractedEtags => _stats.LastExtractedEtags;

        public long LastLoadedEtag => _stats.LastLoadedEtag;

        public int NumberOfLoadedItems => _stats.NumberOfLoadedItems;

        public int TransformationErrorCount => _stats.TransformationErrorCount;

        public string ChangeVector => _stats.ChangeVector;

        public string BatchCompleteReason => _stats.BatchCompleteReason;

        public void RecordExtractedItem(EtlItemType itemType)
        {
            _stats.NumberOfExtractedItems[itemType]++;
        }

        public void RecordTransformedItem(EtlItemType itemType, bool isTombstone)
        {
            if (isTombstone)
                _stats.NumberOfTransformedTombstones[itemType]++;
            else
                _stats.NumberOfTransformedItems[itemType]++;
        }

        public void RecordLastExtractedEtag(long etag, EtlItemType type)
        {
            Debug.Assert(type != EtlItemType.None);

            var current = _stats.LastExtractedEtags[type];

            if (etag > current)
                _stats.LastExtractedEtags[type] = etag;
        }

        public void RecordLastTransformedEtag(long etag, EtlItemType type)
        {
            Debug.Assert(type != EtlItemType.None);

            var current = _stats.LastTransformedEtags[type];

            if (etag > current)
                _stats.LastTransformedEtags[type] = etag;
        }

        public void RecordChangeVector(string changeVector)
        {
            _stats.ChangeVector = ChangeVectorUtils.MergeVectors(_stats.ChangeVector, changeVector);
        }

        public void RecordLastLoadedEtag(long etag)
        {
            if (etag > _stats.LastLoadedEtag)
                _stats.LastLoadedEtag = etag;
        }

        public void RecordLastFilteredOutEtag(long etag, EtlItemType type)
        {
            Debug.Assert(type != EtlItemType.None);

            var current = _stats.LastFilteredOutEtags[type];

            if (etag > current)
                _stats.LastFilteredOutEtags[type] = etag;
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

        public bool HasBatchCompleteReason()
        {
            return string.IsNullOrEmpty(_stats.BatchCompleteReason) == false;
        }

        public long GetLastTransformedOrFilteredEtag(EtlItemType type)
        {
            return Math.Max(LastTransformedEtags[type], LastFilteredOutEtags[type]);
        }

        public void RecordTransformationError()
        {
            _stats.TransformationErrorCount++;
        }

        public void RecordLoadSuccess(int count)
        {
            _stats.SuccessfullyLoaded = true;
            _stats.NumberOfLoadedItems = count;
        }

        public void RecordLoadFailure()
        {
            _stats.SuccessfullyLoaded = false;
        }

        public void RecordCurrentlyAllocated(long allocatedInBytes)
        {
            _stats.CurrentlyAllocated = new Size(allocatedInBytes);
        }
    }
}
