using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class DocumentTimeSeriesOperation : ICommandData
    {
        public List<AppendTimeSeriesOperation> Appends;
        public List<RemoveTimeSeriesOperation> Removals;
        public string Id { get; set; }

        public string Name => null;

        public string ChangeVector => null;

        public CommandType Type => CommandType.TimeSeries;

        public DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Type)] = Type
            };
            if (Appends != null)
            {
                result[nameof(Appends)] = new DynamicJsonArray(Appends.Select(x => x.ToJson()));
            }
            if (Removals != null)
            {
                result[nameof(Removals)] = new DynamicJsonArray(Removals.Select(x => x.ToJson()));
            }
            return result;
        }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return ToJson();
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            
        }
    }
}
