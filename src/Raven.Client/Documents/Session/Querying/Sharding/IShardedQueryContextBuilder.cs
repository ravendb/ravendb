using System.Collections.Generic;

namespace Raven.Client.Documents.Session.Querying.Sharding;

public interface IShardedQueryContextBuilder
{
    IShardedQueryContextBuilder ByDocumentId(string id);

    IShardedQueryContextBuilder ByDocumentIds(ICollection<string> ids);
}
