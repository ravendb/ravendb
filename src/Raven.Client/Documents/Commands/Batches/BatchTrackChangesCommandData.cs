using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    internal sealed class BatchTrackChangesCommandData : ICommandData
    {
        internal readonly Dictionary<string, string> TrackedEntities;
        private readonly HashSet<string> _idsToSkip;
        public string Id => throw new NotSupportedException();
        public string Name { get; } = null;
        public string ChangeVector => throw new NotSupportedException();

        public CommandType Type { get; } = CommandType.BatchTrackChanges;

        public BatchTrackChangesCommandData(Dictionary<string, string> trackedEntities, HashSet<string> idsToSkip)
        {
            TrackedEntities = trackedEntities;
            _idsToSkip = idsToSkip;
        }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var trackedEntitiesJson = new DynamicJsonValue();

            foreach (var kvp in TrackedEntities)
            {
                if (_idsToSkip.Contains(kvp.Key))
                    continue;

                trackedEntitiesJson[kvp.Key] = kvp.Value;
            }

            var json = new DynamicJsonValue
            {
                [nameof(Type)] = Type.ToString(),
                [nameof(TrackedEntities)] = trackedEntitiesJson,
            };
            return json;
        }

        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
            // this command does not update session state after SaveChanges call!
        }
    }
}
