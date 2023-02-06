using System;
using System.Linq;
using Raven.Client.Documents.Session.Querying.Sharding;

namespace Raven.Client.Documents.Session;

public abstract partial class AbstractDocumentQuery<T, TSelf>
{
    protected void ShardContext(Action<IShardedQueryContextBuilder> builder)
    {
        var builderImpl = new ShardedQueryContextBuilder();

        builder.Invoke(builderImpl);

        object shardContext;

        if (builderImpl.DocumentIds.Count == 1)
            shardContext = builderImpl.DocumentIds.First();
        else
            shardContext = builderImpl.DocumentIds;

        QueryParameters.Add(Constants.Documents.Querying.Sharding.ShardContextParameterName, shardContext);
    }
}
