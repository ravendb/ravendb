using System.Runtime.Serialization.Formatters;
using Newtonsoft.Json;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Data
{
    public class BulkOperationResult : IOperationResult
    {
        public long Total { get; set; }

        public string Message => $"Processed ${Total} items.";
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Total"] = Total,
                ["Message"] = Message,
            };
        }
    }
}