using System;
using System.Linq;
using Raven.Client.Documents.Session.Querying.Sharding;

namespace Raven.Client.Documents.Session;

public abstract partial class AbstractDocumentQuery<T, TSelf>
{
    protected void ShardContext(Action<IQueryShardedContextBuilder> builder)
    {
        var builderImpl = new QueryShardedContextBuilder();

        builder.Invoke(builderImpl);

        QueryParameters.Add(Constants.Documents.Querying.Sharding.ShardContextParameterName, new
        {
            builderImpl.DocumentIds,
            builderImpl.Prefixes
        });
    }
}
