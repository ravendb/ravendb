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
    public class ParallelShardAccessStrategy: IShardAccessStrategy
    {
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
