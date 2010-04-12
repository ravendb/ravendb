using System;
using System.Collections.Generic;
using Raven.Client.Document;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
    public interface IShardAccessStrategy
    {
        IList<T> Apply<T>(
			IList<IDocumentSession> shardSessions, 
			Func<IDocumentSession, IList<T>> operation
			);
    }
}
