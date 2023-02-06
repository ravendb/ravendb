using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Session.Querying.Sharding;

internal class ShardedQueryContextBuilder : IShardedQueryContextBuilder
{
    public HashSet<string> DocumentIds { get; } = new(StringComparer.OrdinalIgnoreCase);

    public IShardedQueryContextBuilder ByDocumentId(string id)
    {
        DocumentIds.Add(id);

        return this;
    }

    public IShardedQueryContextBuilder ByDocumentIds(ICollection<string> ids)
    {
        foreach (string id in ids)
        {
            DocumentIds.Add(id);
        }

        return this;
    }
}
