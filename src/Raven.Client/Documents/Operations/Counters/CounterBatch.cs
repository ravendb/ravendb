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
        public List<DocumentCountersOperation> Documents = new List<DocumentCountersOperation>();
        public bool FromEtl;
    }

    public class DocumentCountersOperation
    {
        public List<CounterOperation> Operations;
        public string DocumentId;
        
        public static DocumentCountersOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet("DocumentId", out string docId) == false || docId == null)
                ThrowMissingDocumentId();

            if (input.TryGet("Operations", out BlittableJsonReaderArray operations) == false || operations == null)
                ThrowMissingCounterOperations();

            var result = new DocumentCountersOperation
            {
                DocumentId = docId,
                Operations = new List<CounterOperation>()
            };

            foreach (var op in operations)
            {
                if (!(op is BlittableJsonReaderObject bjro))
                {
                    ThrowNotBlittableJsonReaderObjectOperation(op);
                    return null; //never hit
                }

                result.Operations.Add(CounterOperation.Parse(bjro));
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
                [nameof(Operations)] = Operations?.Select(x => x.ToJson())
            };
        }
    }

    public enum CounterOperationType
    {
        None,
        Increment,
        Delete,
        Get,
        Put
    }

    public class CounterOperation
    {
        public CounterOperationType Type;
        public string CounterName;
        public long Delta;

        internal string ChangeVector;

        public static CounterOperation Parse(BlittableJsonReaderObject input)
        {
            if (input.TryGet(nameof(CounterName), out string name) == false || name == null)
                ThrowMissingCounterName();

            if (input.TryGet(nameof(Type), out string type) == false || type == null)
                ThrowMissingCounterOperationType(name);

            var counterOperationType = (CounterOperationType)Enum.Parse(typeof(CounterOperationType), type);

            long? delta = null;
            switch (counterOperationType)
            {
                case CounterOperationType.Increment:
                case CounterOperationType.Put:
                    if (input.TryGet(nameof(Delta), out delta) == false)
                        ThrowMissingDeltaProperty(name, counterOperationType);
                    break;
            }

            var counterOperation = new CounterOperation
            {
                Type = counterOperationType,
                CounterName = name
            };

            if (delta != null)
                counterOperation.Delta = delta.Value;

            return counterOperation;
        }

        private static void ThrowMissingDeltaProperty(string name, CounterOperationType type)
        {
            throw new InvalidDataException($"Missing '{nameof(Delta)}' property in Counter '{name}' of Type {type} ");
        }

        private static void ThrowMissingCounterOperationType(string name)
        {
            throw new InvalidDataException($"Missing '{nameof(Type)}' property in Counter '{name}'");
        }

        private static void ThrowMissingCounterName()
        {
            throw new InvalidDataException($"Missing '{nameof(CounterName)}' property");
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
