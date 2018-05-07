using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class CountersBatchCommandData : ICommandData
    {
        public CountersBatchCommandData(string documentId, CounterOperation counterOperation)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (counterOperation == null)
                throw new ArgumentNullException(nameof(counterOperation));

            Id = documentId;
            Name = null;
            ChangeVector = null;

            Counters = new DocumentCountersOperation
            {
                DocumentId = documentId,
                Operations = new List<CounterOperation> { counterOperation }
            };
        }

        public string Id { get; }
        public string Name { get; }
        public string ChangeVector { get; }

        public DocumentCountersOperation Counters { get;  }
        public CommandType Type { get; } = CommandType.Counters;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Counters)] = Counters.ToJson(),
                [nameof(Type)] = Type.ToString()
            };
        }
    }
}
