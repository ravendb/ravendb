using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Shard
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
