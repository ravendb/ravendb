using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public interface IOperationProgress
    {
        DynamicJsonValue ToJson();
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
                ["Total"] = Total,
            };
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
                ["Processed"] = Processed,
            };
        }
    }

    public class BulkInsertProgress : IOperationProgress
    {
        public long Processed { get; set; }

        public long TxMergerCalled { get; set; }

        public string LastProcessedId = "not found";

        public override string ToString()
        {
            return $"TxMerger was {TxMergerCalled} called and inserted {Processed} documents with the last known id of {LastProcessedId}";
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Processed"] = Processed,
                ["TxMergerCalled"] = TxMergerCalled,
                ["LastProcessedId"] = LastProcessedId
            };
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
    }
}