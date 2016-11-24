using System.Runtime.Serialization.Formatters;
using Newtonsoft.Json;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Data
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

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Processed"] = Processed,
                ["Total"] = Total,
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
;        }
    }
}