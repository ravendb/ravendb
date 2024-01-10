using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Rachis
{
    public sealed class ClusterTransactionWaiter : AsyncWaiter
    {
        public RemoveTask CreateTask(string id, out TaskCompletionSource tcs)
        {
            tcs = _results.GetOrAdd(id, static (key) => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            return new RemoveTask(this, id);
        }
    }

    public class AsyncWaiter
    {
        protected readonly ConcurrentDictionary<string, TaskCompletionSource> _results = new ConcurrentDictionary<string, TaskCompletionSource>();

        public RemoveTask CreateTask(out string id)
        {
            id = Guid.NewGuid().ToString();
            _results.TryAdd(id, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            return new RemoveTask(this, id);
        }

        public TaskCompletionSource Get(string id)
        {
            _results.TryGetValue(id, out var val);
            return val;
        }

        public void TrySetResult(string id)
        {
            if (_results.TryGetValue(id, out var task))
            {
                task.TrySetResult();
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
            private readonly AsyncWaiter _parent;
            private readonly string _id;

            public RemoveTask(AsyncWaiter parent, string id)
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
