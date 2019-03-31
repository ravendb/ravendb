using System.Collections.Generic;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class DocumentTimeSeriesOperation
    {
        public List<AppendTimeSeriesOperation> Appends;
        public List<RemoveTimeSeriesOperation> Removals;
        public string DocumentId;
        
        public static DocumentTimeSeriesOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet("DocumentId", out string docId) == false || docId == null)
                ThrowMissingDocumentId();

            if (input.TryGet("Appends", out BlittableJsonReaderArray appends) == false )
                ThrowMissingCounterOperations();
            
            if (input.TryGet("Removals", out BlittableJsonReaderArray removals) == false )
                ThrowMissingCounterOperations();

            var result = new DocumentTimeSeriesOperation
            {
                DocumentId = docId,
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
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(Appends)] = Appends,
                [nameof(Removals)] = Removals
            };
        }
    }
}
