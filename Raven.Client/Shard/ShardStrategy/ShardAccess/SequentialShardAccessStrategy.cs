using System;
using System.Collections.Generic;
using Raven.Client.Document;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
    public class SequentialShardAccessStrategy : IShardAccessStrategy
    {
        public IList<T> Apply<T>(IList<IDocumentSession> shardSessions, Func<IDocumentSession, IList<T>> operation)
        {
            var returnList = new List<T>();

            foreach (var shardSession in shardSessions)
            {
                returnList.AddRange(operation(shardSession));
            }

            return returnList;
        }
    }
}
