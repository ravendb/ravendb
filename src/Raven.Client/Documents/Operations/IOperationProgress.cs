using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public interface IOperationProgress
    {
        DynamicJsonValue ToJson();

        IOperationProgress Clone();

        bool CanMerge { get; }

        void MergeWith(IOperationProgress progress);
    }

    public interface IShardedOperationProgress : IOperationProgress
    {
        public int ShardNumber { get; set; }
        public string NodeTag { get; set; }
        
        void Fill(IOperationProgress progress, int shardNumber, string nodeTag);
    }

    /// <summary>
    /// Used to describe operations with progress expressed as percentage (using processed / total items)
    /// </summary>
    public class DeterminateProgress : IOperationProgress
    {
        public long Processed { get; set; }
        public long Total { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Processed"] = Processed,
                ["Total"] = Total
            };
        }

        IOperationProgress IOperationProgress.Clone()
        {
            return new DeterminateProgress
            {
                Processed = Processed,
                Total = Total
            };
        }

        bool IOperationProgress.CanMerge => true;

        void IOperationProgress.MergeWith(IOperationProgress progress)
        {
            if (progress is not DeterminateProgress p)
                return;

            Processed += p.Processed;
            Total += p.Total;
        }
    }

    /// <summary>
    /// Used to describe operations with progress expressed as # of total items processes
    /// </summary>
    public class IndeterminateProgressCount : IOperationProgress
    {
        public long Processed { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Processed"] = Processed
            };
        }

        IOperationProgress IOperationProgress.Clone()
        {
            return new IndeterminateProgressCount
            {
                Processed = Processed
            };
        }

        bool IOperationProgress.CanMerge => true;

        void IOperationProgress.MergeWith(IOperationProgress progress)
        {
            if (progress is not IndeterminateProgressCount p)
                return;

            Processed += p.Processed;
        }
    }

    public class BulkInsertProgress : IOperationProgress
    {
        public long Total { get; set; }
        public long BatchCount { get; set; }
        public string LastProcessedId { get; set; }

        [Obsolete("Use field DocumentsProcessed instead")]
        public long Processed { get; set; }

        public long DocumentsProcessed { get; set; }
        public long AttachmentsProcessed { get; set; }
        public long CountersProcessed { get; set; }
        public long TimeSeriesProcessed { get; set; }

        public override string ToString()
        {
            var msg = $"Inserted {Total:#,#;;0} items in {BatchCount:#,#;;0} batches.";

            if (LastProcessedId != null)
                msg += $" Last document ID: '{LastProcessedId}'";

            return msg;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Total)] = Total,
                [nameof(BatchCount)] = BatchCount,
                [nameof(LastProcessedId)] = LastProcessedId,
#pragma warning disable CS0618 // Type or member is obsolete
                [nameof(Processed)] = DocumentsProcessed,
#pragma warning restore CS0618 // Type or member is obsolete
                [nameof(DocumentsProcessed)] = DocumentsProcessed,
                [nameof(AttachmentsProcessed)] = AttachmentsProcessed,
                [nameof(CountersProcessed)] = CountersProcessed,
                [nameof(TimeSeriesProcessed)] = TimeSeriesProcessed
            };
        }

        IOperationProgress IOperationProgress.Clone()
        {
            throw new NotImplementedException();
        }

        bool IOperationProgress.CanMerge => false;

        void IOperationProgress.MergeWith(IOperationProgress progress)
        {
            throw new NotSupportedException();
        }
    }

    /// <summary>
    /// Used to describe indeterminate progress (we use text to describe progress)
    /// </summary>
    public class IndeterminateProgress : IOperationProgress
    {
        public string Progress { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Progress"] = Progress
            };
        }

        IOperationProgress IOperationProgress.Clone()
        {
            throw new NotImplementedException();
        }

        bool IOperationProgress.CanMerge => false;

        void IOperationProgress.MergeWith(IOperationProgress progress)
        {
            throw new NotSupportedException();
        }
    }
}
