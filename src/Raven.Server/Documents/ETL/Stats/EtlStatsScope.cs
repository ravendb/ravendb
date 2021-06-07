using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Utils;
using Raven.Server.Utils.Stats;
using Sparrow;

namespace Raven.Server.Documents.ETL.Stats
{
    public class OlapEtlStatsScope : AbstractEtlStatsScope<OlapEtlStatsScope, OlapEtlPerformanceOperation>
    {
        public OlapEtlStatsScope(EtlRunStats stats, bool start = true) : base(stats, start)
        {
        }

        public UploadProgress AzureUpload { get; set; }

        public UploadProgress FtpUpload { get; set; }

        public UploadProgress GlacierUpload { get; set; }

        public UploadProgress GoogleCloudUpload { get; set; }

        public UploadProgress S3Upload { get; set; }

        public int NumberOfFiles { get; set; }

        public string FileName { get; set; }

        protected override OlapEtlStatsScope OpenNewScope(EtlRunStats stats, bool start)
        {
            return new OlapEtlStatsScope(stats, start);
        }

        protected override OlapEtlPerformanceOperation ToPerformanceOperation(string name, OlapEtlStatsScope scope)
        {
            return scope.ToPerformanceOperation(name);
        }

        public override OlapEtlPerformanceOperation ToPerformanceOperation(string name)
        {
            var operation = new OlapEtlPerformanceOperation(Duration)
            {
                Name = name,
                AzureUpload = AzureUpload,
                FtpUpload = FtpUpload,
                GlacierUpload = GlacierUpload,
                GoogleCloudUpload = GoogleCloudUpload,
                S3Upload = S3Upload,
                NumberOfFiles = NumberOfFiles,
                FileName = FileName
            };

            if (Scopes != null)
            {
                operation.Operations = Scopes
                    .Select(x => ToPerformanceOperation(x.Key, x.Value))
                    .ToArray();
            }

            return operation;
        }
    }

    public class EtlStatsScope : AbstractEtlStatsScope<EtlStatsScope, EtlPerformanceOperation>
    {
        public EtlStatsScope(EtlRunStats stats, bool start = true)
            : base(stats, start)
        {
        }

        protected override EtlStatsScope OpenNewScope(EtlRunStats stats, bool start)
        {
            return new EtlStatsScope(stats, start);
        }

        protected override EtlPerformanceOperation ToPerformanceOperation(string name, EtlStatsScope scope)
        {
            return scope.ToPerformanceOperation(name);
        }

        public override EtlPerformanceOperation ToPerformanceOperation(string name)
        {
            var operation = new EtlPerformanceOperation(Duration)
            {
                Name = name
            };

            if (Scopes != null)
            {
                operation.Operations = Scopes
                    .Select(x => ToPerformanceOperation(x.Key, x.Value))
                    .ToArray();
            }

            return operation;
        }
    }

    public abstract class AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation> : StatsScope<EtlRunStats, TStatsScope>
        where TStatsScope : StatsScope<EtlRunStats, TStatsScope>
        where TEtlPerformanceOperation : EtlPerformanceOperation
    {
        private readonly EtlRunStats _stats;

        protected AbstractEtlStatsScope(EtlRunStats stats, bool start = true) : base(stats, start)
        {
            _stats = stats;
        }

        public Dictionary<EtlItemType, int> NumberOfExtractedItems => _stats.NumberOfExtractedItems;

        public Dictionary<EtlItemType, int> NumberOfTransformedItems => _stats.NumberOfTransformedItems;

        public Dictionary<EtlItemType, int> NumberOfTransformedTombstones => _stats.NumberOfTransformedTombstones;

        public Dictionary<EtlItemType, long> LastTransformedEtags => _stats.LastTransformedEtags;

        public Dictionary<EtlItemType, long> LastFilteredOutEtags => _stats.LastFilteredOutEtags;

        public Dictionary<EtlItemType, long> LastExtractedEtags => _stats.LastExtractedEtags;

        public long LastLoadedEtag => _stats.LastLoadedEtag;

        public int NumberOfLoadedItems => _stats.NumberOfLoadedItems;

        public int TransformationErrorCount => _stats.TransformationErrorCount;

        public string ChangeVector => _stats.ChangeVector;

        public string BatchCompleteReason => _stats.BatchCompleteReason;

        public Size BatchSize => _stats.BatchSize;

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

        protected abstract TEtlPerformanceOperation ToPerformanceOperation(string name, TStatsScope scope);

        public abstract TEtlPerformanceOperation ToPerformanceOperation(string name);

        public void RecordBatchCompleteReason(string reason)
        {
            _stats.BatchCompleteReason = reason;
        }

        public bool HasBatchCompleteReason()
        {
            return string.IsNullOrEmpty(_stats.BatchCompleteReason) == false;
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
            _stats.CurrentlyAllocated = new Size(allocatedInBytes, SizeUnit.Bytes);
        }

        public void IncrementBatchSize(long sizeInBytes)
        {
            _stats.BatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }
    }
}
