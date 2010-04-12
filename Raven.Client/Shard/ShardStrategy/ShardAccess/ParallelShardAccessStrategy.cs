using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
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

    internal static class ParallelExtensions
    {
        public static void WaitAll(this IEnumerable<Task> tasks)
        {
            try
            {
                Task.WaitAll(tasks.ToArray());
            }
            catch (Exception ex)
            {
                //when task takes exception it wraps in aggregate exception, if in continuation
                //then could be double wrapped, etc. This should always get us the original
                while (true)
                {
                	if (ex.InnerException == null || !(ex is AggregateException))
                	{
						throw PreserveStackTrace(ex);
                	}
                	ex = ex.InnerException;
                }
            }
        }

    	private static Exception PreserveStackTrace(Exception exception)
    	{
    		typeof (Exception).InvokeMember("InternalPreserveStackTrace", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.InvokeMethod, null,
    		                                exception, null);
    		return exception;
    	}
    }
}
