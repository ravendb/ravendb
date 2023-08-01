using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public sealed class CountersBatchCommandData : ICommandData
    {
        public CountersBatchCommandData(string documentId, CounterOperation counterOperation)
            : this(documentId, new List<CounterOperation>
            {
                counterOperation
            })
        {
            if (counterOperation == null)
                throw new ArgumentNullException(nameof(counterOperation));
        }

        public CountersBatchCommandData(string documentId, List<CounterOperation> counterOperations)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            
            Id = documentId;
            Name = null;
            ChangeVector = null;

            Counters = new DocumentCountersOperation
            {
                DocumentId = documentId,
                Operations = counterOperations
            };
        }

        public string Id { get; }
        public string Name { get; }
        public string ChangeVector { get; }

        internal bool? FromEtl { get; set; }

        public DocumentCountersOperation Counters { get;  }
        public CommandType Type { get; } = CommandType.Counters;

        public bool HasDelete(string counterName)
        {
            return HasOperationOfType(CounterOperationType.Delete, counterName);
        }

        public bool HasIncrement(string counterName)
        {
            return HasOperationOfType(CounterOperationType.Increment, counterName);
        }

        private bool HasOperationOfType(CounterOperationType type, string counterName)
        {
            foreach (var op in Counters.Operations)
            {
                if (op.CounterName != counterName)
                    continue;
                if (op.Type == type)
                    return true;
            }

            return false;
        }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var result = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Counters)] = Counters.ToJson(),
                [nameof(Type)] = Type.ToString()
            };

            if (FromEtl.HasValue)
                result[nameof(FromEtl)] = FromEtl;

            return result;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
