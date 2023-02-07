using System.Collections.Generic;

namespace Raven.Client.Documents.Session.Querying.Sharding;

public interface IQueryShardedContextBuilder
{
    IQueryShardedContextBuilder ByDocumentId(string id);

    IQueryShardedContextBuilder ByDocumentIds(IEnumerable<string> ids);
}
