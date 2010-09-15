using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
	/// <summary>
	/// Apply an operation to all the shard session in parallel
	/// </summary>
    public class ParallelShardAccessStrategy: IShardAccessStrategy
    {
		/// <summary>
		/// Applies the specified action to all shard sessions in parallel
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="shardSessions">The shard sessions.</param>
		/// <param name="operation">The operation.</param>
		/// <returns></returns>
        public IList<T> Apply<T>(IList<IDocumentSession> shardSessions, Func<IDocumentSession, IList<T>> operation)
        {
        	var returnedLists = new IList<T>[shardSessions.Count];

			shardSessions
				.Select((shardSession,i) =>
					Task.Factory
						.StartNew(() => operation(shardSession))
						.ContinueWith(task =>
						{
							returnedLists[i] = task.Result;
						})
				)
				.WaitAll();

        	return returnedLists
				.Where(x => x != null)
        		.SelectMany(x => x)
				.ToArray();
        }
    }
}
