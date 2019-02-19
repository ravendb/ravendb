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
                ["Total"] = Total
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
                ["Processed"] = Processed
            };
        }
    }

    public class BulkInsertProgress : IOperationProgress
    {
        public long Processed { get; set; }

        public long BatchCount { get; set; }

        public string LastProcessedId { get; set; }

        public override string ToString()
        {
            var msg = $"Inserted {Processed} documents in {BatchCount} batches.";

            if (LastProcessedId != null)
                msg += $" Last document id: '{LastProcessedId}'";

            return msg;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Processed)] = Processed,
                [nameof(BatchCount)] = BatchCount,
                [nameof(LastProcessedId)] = LastProcessedId
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
