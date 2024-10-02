using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Rachis
{
    public sealed class ClusterTransactionWaiter : AsyncWaiter<HashSet<string>>
    {
        public RemoveTask CreateTask(string id, out TaskCompletionSource<HashSet<string>> tcs)
        {
            tcs = _results.GetOrAdd(id, static (key) => new TaskCompletionSource<HashSet<string>>(TaskCreationOptions.RunContinuationsAsynchronously));
            return new RemoveTask(this, id);
        }
    }

    public class AsyncWaiter<T>
    {
        protected readonly ConcurrentDictionary<string, TaskCompletionSource<T>> _results = new ConcurrentDictionary<string, TaskCompletionSource<T>>();

        public RemoveTask CreateTask(out string id)
        {
            id = Guid.NewGuid().ToString();
            _results.TryAdd(id, new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously));
            return new RemoveTask(this, id);
        }

        public TaskCompletionSource<T> Get(string id)
        {
            _results.TryGetValue(id, out var val);
            return val;
        }

        public void TrySetResult(string id, T result)
        {
            if (_results.TryGetValue(id, out var task))
            {
                task.TrySetResult(result);
            }
        }

        public void TrySetException(string id, Exception e)
        {
            if (_results.TryGetValue(id, out var task))
            {
                task.TrySetException(e);
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
                _parent._results.TryRemove(_id, out var task);
                // cancel it, if someone still awaits
                task?.TrySetCanceled();
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
