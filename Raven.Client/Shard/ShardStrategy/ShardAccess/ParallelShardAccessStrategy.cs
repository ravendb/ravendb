using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
    public class ParallelShardAccessStrategy: IShardAccessStrategy
    {
        public IList<T> Apply<T>(IList<IDocumentSession> shardSessions, Func<IDocumentSession, IList<T>> operation)
        {
            var returnList = new List<T>();

            //List.AddRange not threadsafe, make sure addrange calls don't happen concurrently
            object lockObject = new object();

            shardSessions
                .Select(shardSession => 
                    Task.Factory
                        .StartNew(() => operation(shardSession))
                        .AddToListOnComplete(lockObject, returnList)
                )
                .WaitAll()
            ;

            return returnList;
        }
    }

    internal static class ParallelExtensions
    {
        public static Task AddToListOnComplete<T>(this Task<IList<T>> task, object lockObject, List<T> returnList)
        {
            return task.ContinueWith(x => {
                lock (lockObject)
                {
                    if (x.Result != null)
                        returnList.AddRange(x.Result);
                }
            });
        }

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
                        throw ex;
                    else
                        ex = ex.InnerException;
                }
            }
        }
    }
}
