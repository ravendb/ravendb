using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;


namespace Raven.Client.Documents.Commands.Batches
{
    internal class CopyTimeSeriesCommandData : ICommandData
    {
        public CopyTimeSeriesCommandData(string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName)
        {
            if (string.IsNullOrWhiteSpace(sourceDocumentId))
                throw new ArgumentNullException(nameof(sourceDocumentId));
            if (string.IsNullOrWhiteSpace(sourceName))
                throw new ArgumentNullException(nameof(sourceName));
            if (string.IsNullOrWhiteSpace(destinationDocumentId))
                throw new ArgumentNullException(nameof(destinationDocumentId));
            if (string.IsNullOrWhiteSpace(destinationName))
                throw new ArgumentNullException(nameof(destinationName));


            Id = sourceDocumentId;
            Name = sourceName;
            DestinationId = destinationDocumentId;
            DestinationName = destinationName;
        }
        public string Id { get; }
        public string Name { get; }
        public string ChangeVector { get; }
        public string DestinationId { get; }
        public string DestinationName { get; }
        public CommandType Type { get; } = CommandType.TimeSeriesCopy;

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(DestinationId)] = DestinationId,
                [nameof(DestinationName)] = DestinationName,
                [nameof(Type)] = Type.ToString()
            };
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
