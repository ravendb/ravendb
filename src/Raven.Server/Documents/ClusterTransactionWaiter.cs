using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Server.Documents
{
    public class ClusterTransactionWaiter
    {
        internal readonly DocumentDatabase Database;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<object>> _results = new ConcurrentDictionary<string, TaskCompletionSource<object>>();

        public ClusterTransactionWaiter(DocumentDatabase database)
        {
            Database = database;
        }

        public RemoveTask CreateTask(out string id)
        {
            id = Guid.NewGuid().ToString();
            _results.TryAdd(id, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
            return new RemoveTask(this, id);
        }

        public TaskCompletionSource<object> Get(string id)
        {
            _results.TryGetValue(id, out var val);
            return val;
        }

        public void SetResult(string id, long index, object result)
        {
            Database.RachisLogIndexNotifications.NotifyListenersAbout(index, null);
            if (_results.TryGetValue(id, out var task))
            {
                task.SetResult(result);
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

        public Task<object> WaitForResults(string id, CancellationToken token)
        {
            if (_results.TryGetValue(id, out var task) == false)
            {
                throw new InvalidOperationException($"Task with the id '{id}' was not found.");
            }

            token.Register(() => task.TrySetCanceled());
            return task.Task;
        }
    }
}
