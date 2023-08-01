using System.Collections.Generic;
using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Raven.Client.Documents.Operations.Revisions;
using Sparrow.Utils;

namespace Raven.Server.Documents.Includes.Sharding;

public sealed class ShardedRevisionIncludes : IRevisionIncludes
{
    private Dictionary<string, List<BlittableJsonReaderObject>> _revisionsByDocumentId;

    public int Count => _revisionsByDocumentId?.Count ?? 0;

    public void AddResults(BlittableJsonReaderArray results, JsonOperationContext contextToClone)
    {
        if (results == null || results.Length == 0)
            return;

        _revisionsByDocumentId ??= new Dictionary<string, List<BlittableJsonReaderObject>>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < results.Length; i++)
        {
            var json = results.GetByIndex<BlittableJsonReaderObject>(i);

            if (json.TryGet(nameof(RevisionIncludeResult.Id), out string docId) == false)
                throw new InvalidOperationException($"Could not find revision's {nameof(RevisionIncludeResult.Id)}");

            if (_revisionsByDocumentId.TryGetValue(docId, out var revisions) == false)
                _revisionsByDocumentId[docId] = revisions = new List<BlittableJsonReaderObject>();

            revisions.Add(json.Clone(contextToClone));
        }

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "We can get duplicated revision includes when resharding is running. How to deal with that?");
    }

    public async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token)
    {
        var first = true;

        foreach (var kvp in _revisionsByDocumentId)
        {
            foreach (var revision in kvp.Value)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WriteObject(revision);

                await writer.MaybeFlushAsync(token);
            }
        }
    }
}
