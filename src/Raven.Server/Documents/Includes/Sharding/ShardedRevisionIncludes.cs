using System.Collections.Generic;
using System;
using Sparrow.Json;
using Raven.Client.Documents.Operations.Revisions;

namespace Raven.Server.Documents.Includes.Sharding;

public class ShardedRevisionIncludes : IIncludeRevisions
{
    public Dictionary<string, Document> RevisionsChangeVectorResults { get; private set; }

    public Dictionary<string, Dictionary<DateTime, Document>> IdByRevisionsByDateTimeResults { get; private set; }

    public void AddResults(BlittableJsonReaderArray results, JsonOperationContext contextToClone)
    {
        if (results == null)
            return;

        for (var i = 0; i < results.Length; i++)
        {
            var revisionsJson = results.GetByIndex<BlittableJsonReaderObject>(i);

            var revision = CreateRevisionFrom(revisionsJson, contextToClone, out var before);

            if (before != null)
            {
                IdByRevisionsByDateTimeResults ??= new Dictionary<string, Dictionary<DateTime, Document>>(StringComparer.OrdinalIgnoreCase);

                if (IdByRevisionsByDateTimeResults.TryGetValue(revision.Id, out var revisionsByDateTime) == false)
                    IdByRevisionsByDateTimeResults[revision.Id] = revisionsByDateTime = new Dictionary<DateTime, Document>();

                revisionsByDateTime[before.Value] = revision;
            }
            else
            {
                RevisionsChangeVectorResults ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);

                RevisionsChangeVectorResults[revision.ChangeVector] = revision;
            }
        }
    }

    private static Document CreateRevisionFrom(BlittableJsonReaderObject json, JsonOperationContext context, out DateTime? before)
    {
        if (json.TryGet(nameof(RevisionIncludeResult.Id), out LazyStringValue id) == false)
            throw new InvalidOperationException($"Could not find revision's {nameof(RevisionIncludeResult.Id)}");

        if (json.TryGet(nameof(RevisionIncludeResult.ChangeVector), out string changeVector) == false)
            throw new InvalidOperationException($"Could not find revision's {nameof(RevisionIncludeResult.ChangeVector)}");

        if (json.TryGet(nameof(RevisionIncludeResult.Revision), out BlittableJsonReaderObject revision) == false)
            throw new InvalidOperationException($"Could not find revision's body");

        json.TryGet(nameof(RevisionIncludeResult.Before), out before);

        return new Document
        {
            Id = id.Clone(context), 
            ChangeVector = changeVector,
            Data = revision.Clone(context)
        };
    }
}
