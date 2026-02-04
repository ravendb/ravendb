using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    internal sealed class BatchTrackChangesCommandData : ICommandData
    {
        internal readonly Dictionary<string, string> TrackedEntities;
        public string Id => throw new NotSupportedException();
        public string Name { get; } = null;
        public string ChangeVector => throw new NotSupportedException();

        public CommandType Type { get; } = CommandType.BatchTrackChanges;

        public BatchTrackChangesCommandData(Dictionary<string, string> trackedEntities)
        {
            TrackedEntities = trackedEntities;
        }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(Type)] = Type.ToString(),
                [nameof(TrackedEntities)] = TrackedEntities.ToJson(),
            };
            return json;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            // this command does not update session state after SaveChanges call!
        }
    }
}
