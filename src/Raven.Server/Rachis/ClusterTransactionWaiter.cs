using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Rachis
{
    public sealed class ClusterTransactionWaiter : AsyncWaiter<long?>
    {
        public RemoveTask CreateTaskForDatabase(string id, long index, DocumentDatabase database, out Task<long?> task)
        {
            var t = new TaskCompletionSource<long?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var current = _results.GetOrAdd(id, t);
        
            if (current == t)
            {
                var lastCompleted = Interlocked.Read(ref database.LastCompletedClusterTransactionIndex);
                if (lastCompleted >= index)
                {
                    current.TrySetResult(null);
                }
            }
        
            task = current.Task;
            return new RemoveTask(this, id);
        }
    }

    public class AsyncWaiter<T>
    {
        protected readonly ConcurrentDictionary<string, TaskCompletionSource<T>> _results = new ConcurrentDictionary<string, TaskCompletionSource<T>>();

        public virtual RemoveTask CreateTask(out string id)
        {
            id = Guid.NewGuid().ToString();
            return CreateTask(id, out _);
        }

        public virtual RemoveTask CreateTask(string id)
        {
            return CreateTask(id, out _);
        }

        public virtual RemoveTask CreateTask(string id, out Task<T> task)
        {
            task = _results.GetOrAdd(id, new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            return new RemoveTask(this, id);
        }

        public TaskCompletionSource<T> Get(string id)
        {
            _results.TryGetValue(id, out var val);
            return val;
        }

        public void SetResult(string id, T result)
        {
            if (_results.TryGetValue(id, out var task))
            {
                task.SetResult(result);
            }
        }

        public void SetException(string id, Exception e)
        {
            if (_results.TryGetValue(id, out var task))
            {
                task.SetException(e);
            }
        }

        public readonly struct RemoveTask : IDisposable
        {
            private readonly AsyncWaiter<T> _parent;
            private readonly string _id;

            public RemoveTask(AsyncWaiter<T> parent, string id)
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

        public async Task<T> WaitForResults(string id, CancellationToken token)
        {
            if (_results.TryGetValue(id, out var task) == false)
            {
                throw new InvalidOperationException($"Task with the id '{id}' was not found.");
            }

            await using (token.Register(() => task.TrySetCanceled()))
            {
                return await task.Task.ConfigureAwait(false);
            }
        }
    }
}
