using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
    public class ParallelShardAccessStrategy: IShardAccessStrategy
    {
        public IList<T> Apply<T>(IList<IDocumentSession> shardSessions, Func<IDocumentSession, IList<T>> operation)
        {
        	var returnList = new ConcurrentStack<T>();

			shardSessions
				.Select(shardSession =>
					Task.Factory
						.StartNew(() => operation(shardSession))
						.ContinueWith(task =>
						{
							if (task.Result == null)
								return;
							returnList.PushRange(task.Result.ToArray());
						})
				)
				.WaitAll()
			;

            return returnList.ToArray();
        }
    }
}
