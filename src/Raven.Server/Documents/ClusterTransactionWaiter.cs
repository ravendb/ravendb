using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Jint;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents
{
    public class ClusterTransactionWaiter
    {
        internal readonly DocumentDatabase Database;
        internal readonly ServerStore ServerStore;
        private readonly ConcurrentDictionary<string, TaskCompletionSource> _results = new ConcurrentDictionary<string, TaskCompletionSource>();

        public ClusterTransactionWaiter(DocumentDatabase database, ServerStore serverStore)
        {
            Database = database;
            ServerStore = serverStore;
        }

        public RemoveTask CreateTask(string id, out Task task)
        {
            var t = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var current = _results.GetOrAdd(id, t);

            long? raftIndex;
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                raftIndex = ServerStore.Engine.LogHistory.GetIndexByRaftId(context, id);
            }

            if (raftIndex.HasValue &&
                Database.LastCompletedClusterTransactionIndex >= raftIndex &&
                current == t)
            {
                // the database already finished the processing of this command
                task = Task.CompletedTask;
            }
            else
            {
                /* the database already finished the processing of this command but didn't removed yet the task
                 * OR
                 * the database is applying the command
                 * OR
                 * I created this task, before the database start applying the tx
                 */
                task = current.Task;
            }

            return new RemoveTask(this, id);
        }

        public TaskCompletionSource Get(string id)
        {
            _results.TryGetValue(id, out var val);
            return val;
        }

        public void SetResult(string id, long index)
        {
            Database.RachisLogIndexNotifications.NotifyListenersAbout(index, null);
            if (_results.TryGetValue(id, out var task))
            {
                task.SetResult();
            }
        }

        public void SetException(string id, long index, Exception e)
        {
            Database.RachisLogIndexNotifications.NotifyListenersAbout(index, e);
            if (_results.TryGetValue(id, out var task))
            {
                task.SetException(e);
            }
        }

        public struct RemoveTask : IDisposable
        {
            private readonly ClusterTransactionWaiter _parent;
            private readonly string _id;

            public RemoveTask(ClusterTransactionWaiter parent, string id)
            {
                _parent = parent;
                _id = id;
            }

            public void Dispose()
            {
                _parent._results.TryRemove(_id, out var task);
                // cancel it, if someone still awaits
                task.TrySetCanceled();
            }
        }

        public async Task WaitForResults(string id, CancellationToken token)
        {
            if (_results.TryGetValue(id, out var task) == false)
            {
                throw new InvalidOperationException($"Task with the id '{id}' was not found.");
            }

            await using (token.Register(() => task.TrySetCanceled()))
            {
                await task.Task.ConfigureAwait(false);
            }
        }
    }
}
