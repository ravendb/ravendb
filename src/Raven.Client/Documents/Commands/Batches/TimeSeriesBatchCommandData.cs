using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public class TimeSeriesBatchCommandData : ICommandData
    {
        public TimeSeriesBatchCommandData(string documentId, List<TimeSeriesOperation.AppendOperation> appends, List<TimeSeriesOperation.RemoveOperation> removals)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));

            Id = documentId;

            TimeSeries = new TimeSeriesOperation
            {
                DocumentId = documentId,
                Appends = appends,
                Removals = removals
            };
        }

        public string Id { get; set; }

        public string Name => null;

        public string ChangeVector => null;

        public CommandType Type => CommandType.TimeSeries;

        public TimeSeriesOperation TimeSeries { get; }

        public DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(TimeSeries)] = TimeSeries.ToJson(),
                [nameof(Type)] = Type
            };

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
