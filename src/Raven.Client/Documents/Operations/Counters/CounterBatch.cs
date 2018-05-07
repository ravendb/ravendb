using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Counters
{
    public class CounterBatch
    {
        public bool ReplyWithAllNodesValues;
        public List<DocumentCountersOperation> Documents;
    }

    public class DocumentCountersOperation
    {
        public List<CounterOperation> Operations;
        public string DocumentId;
        
        public static DocumentCountersOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet("DocumentId", out string docId) == false || docId == null)
                throw new InvalidDataException("Missing 'DocumentId' property on 'Counters'");

            if (input.TryGet("Operations", out BlittableJsonReaderArray operations) == false || operations == null)
                throw new InvalidDataException("Missing 'Operations' property on 'Counters'");

            var result = new DocumentCountersOperation
            {
                DocumentId = docId,
                Operations = new List<CounterOperation>()
            };

            foreach (var op in operations)
            {
                if (!(op is BlittableJsonReaderObject bjro))
                    continue;

                result.Operations.Add(CounterOperation.Parse(bjro));
            }

            return result;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(Operations)] = Operations?.Select(x => x.ToJson())
            };
        }
    }

    public enum CounterOperationType
    {
        None,
        Increment,
        Delete,
        Get
    }

    public class CounterOperation
    {
        public CounterOperationType Type;
        public string CounterName;
        public long Delta;

        public static CounterOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet("CounterName", out string name) == false || name == null)
                throw new InvalidDataException("Missing 'CounterName' property");

            if (input.TryGet("Type", out string type) == false || type == null)
                throw new InvalidDataException($"Missing 'Type' property in Counter '{name}'");

            var typeValue = Enum.Parse(typeof(CounterOperationType), type);
            if (!(typeValue is CounterOperationType counterOperationType))
                throw new InvalidDataException($"Unknown 'CounterOperationType' '{type}' in Counter '{name}'");

            long? delta = null;
            if (counterOperationType == CounterOperationType.Increment && input.TryGet("Delta", out delta) == false)
                throw new InvalidDataException($"Missing 'Delta' property in Counter '{name}' of Type {counterOperationType}  ");

            var counterOperation = new CounterOperation
            {
                Type = counterOperationType,
                CounterName = name
            };

            if (delta != null)
                counterOperation.Delta = delta.Value;

            return counterOperation;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Type)] = Type.ToString(),
                [nameof(CounterName)] = CounterName,
                [nameof(Delta)] = Delta
            };
        }
    }
}
