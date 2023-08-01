using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Session.Querying.Sharding;

internal sealed class QueryShardedContextBuilder : IQueryShardedContextBuilder
{
    public HashSet<string> DocumentIds { get; } = new(StringComparer.OrdinalIgnoreCase);

    public IQueryShardedContextBuilder ByDocumentId(string id)
    {
        DocumentIds.Add(id);

        return this;
    }

    public IQueryShardedContextBuilder ByDocumentIds(IEnumerable<string> ids)
    {
        foreach (string id in ids)
        {
            DocumentIds.Add(id);
        }

        return this;
    }
}
