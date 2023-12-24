using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;

namespace Raven.Server.Rachis
{
    public sealed class ClusterTransactionWaiter : AsyncWaiter
    {

        public RemoveTask CreateTaskForDatabase(string id, long index, DocumentDatabase database, out Task task)
        {
            var t = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var current = _results.GetOrAdd(id, t);
        
            if (current == t)
            {
                var lastCompleted = Interlocked.Read(ref database.LastCompletedClusterTransactionIndex);
                if (lastCompleted >= index)
                {
                    current.TrySetResult();
                }
            }
        
            task = current.Task;
            return new RemoveTask(this, id);
        }
    }

    public class AsyncWaiter
    {
        protected readonly ConcurrentDictionary<string, TaskCompletionSource> _results = new ConcurrentDictionary<string, TaskCompletionSource>();

        public RemoveTask CreateTask(out string id)
        {
            id = Guid.NewGuid().ToString();
            return CreateTask(id, out _);
        }

        public RemoveTask CreateTask(string id)
        {
            return CreateTask(id, out _);
        }

        public RemoveTask CreateTask(string id, out Task task)
        {
            task = _results.GetOrAdd(id, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            return new RemoveTask(this, id);
        }

        public TaskCompletionSource Get(string id)
        {
            _results.TryGetValue(id, out var val);
            return val;
        }

        public bool TrySetResult(string id)
        {
            if (_results.TryGetValue(id, out var task))
            {
                return task.TrySetResult();
            }

            return false;
        }

        public bool TrySetException(string id, Exception e)
        {
            if (_results.TryGetValue(id, out var task))
            {
                return task.TrySetException(e);
            }

            return false;
        }

        public readonly struct RemoveTask : IDisposable
        {
            private readonly AsyncWaiter _parent;
            private readonly string _id;

            public RemoveTask(AsyncWaiter parent, string id)
            {
                _parent = parent;
                _id = id;
            }

            public void Dispose()
            {
                if (_parent._results.TryRemove(_id, out var task))
                {
                    // cancel it, if someone still awaits
                    task.TrySetCanceled();
                }
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
