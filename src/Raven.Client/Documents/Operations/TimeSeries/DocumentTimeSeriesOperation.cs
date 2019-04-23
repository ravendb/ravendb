using System.Collections.Generic;
using System.IO;
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

        public static DocumentTimeSeriesOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet("Id", out string docId) == false || docId == null)
                ThrowMissingDocumentId();

            if (input.TryGet("Appends", out BlittableJsonReaderArray appends) == false )
                ThrowMissingCounterOperations();
            
            if (input.TryGet("Removals", out BlittableJsonReaderArray removals) == false )
                ThrowMissingCounterOperations();

            var result = new DocumentTimeSeriesOperation
            {
                Id = docId,
                Appends = new List<AppendTimeSeriesOperation>(),
                Removals = new List<RemoveTimeSeriesOperation>()
            };
            
            if (appends != null)
            {
                foreach (var op in appends)
                {
                    if (!(op is BlittableJsonReaderObject bjro))
                    {
                        ThrowNotBlittableJsonReaderObjectOperation(op);
                        return null; //never hit
                    }

                    result.Appends.Add(AppendTimeSeriesOperation.Parse(bjro));
                }
            }

            if (removals != null)
            {
                foreach (var op in removals)
                {
                    if (!(op is BlittableJsonReaderObject bjro))
                    {
                        ThrowNotBlittableJsonReaderObjectOperation(op);
                        return null; //never hit
                    }

                    result.Removals.Add(RemoveTimeSeriesOperation.Parse(bjro));
                }
            }
           

            return result;
        }

        private static void ThrowNotBlittableJsonReaderObjectOperation(object op)
        {
            throw new InvalidDataException($"input.Operations should contain items of type BlittableJsonReaderObject only, but got {op.GetType()}");
        }

        private static void ThrowMissingCounterOperations()
        {
            throw new InvalidDataException("Missing 'Operations' property on 'Counters'");
        }

        private static void ThrowMissingDocumentId()
        {
            throw new InvalidDataException("Missing 'DocumentId' property on 'Counters'");
        }

        public DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Type)] = Type
            };
            if(Appends != null)
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
