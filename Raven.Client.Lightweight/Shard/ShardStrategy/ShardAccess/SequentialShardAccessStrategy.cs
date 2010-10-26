using System;
using System.Collections.Generic;
using Raven.Client.Document;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
	/// <summary>
	/// Apply an operation to all the shard session in sequence
	/// </summary>
    public class SequentialShardAccessStrategy : IShardAccessStrategy
    {
		/// <summary>
		/// Applies the specified action for all shard sessions.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="shardSessions">The shard sessions.</param>
		/// <param name="operation">The operation.</param>
		/// <returns></returns>
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
