using System.Runtime.Serialization.Formatters;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Sparrow.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Data
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