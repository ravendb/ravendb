using System;
using Raven.Client.Documents.Session.Querying.Sharding;

namespace Raven.Client.Documents.Session;

public partial class DocumentQuery<T>
{
    IDocumentQuery<T> IDocumentQuery<T>.ShardContext(Action<IShardedQueryContextBuilder> builder)
    {
        ShardContext(builder);
        return this;
    }
}
