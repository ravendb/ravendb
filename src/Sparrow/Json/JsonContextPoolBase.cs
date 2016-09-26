using System;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T>
        where T : JsonOperationContext
    {
        /// <summary>
        /// This is thread static value because we usually have great similiarity in the operations per threads.
        /// Indexing thread will adjust their contexts to their needs, and request processing threads will tend to
        /// average to the same overall type of contexts
        /// </summary>
        private readonly ThreadLocal<Stack<T>> _contextPool = new ThreadLocal<Stack<T>>(() => new Stack<T>(), trackAllValues: true);

        private bool _disposed;

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            T ctx;
            var disposable = AllocateOperationContext(out ctx);
            context = ctx;

            return disposable;
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            var stack = _contextPool.Value;
            context = stack.Count == 0 ? CreateContext() : stack.Pop();

            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };
        }

        protected abstract T CreateContext();

        private class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;

            public void Dispose()
            {
                var stack = Parent._contextPool.Value;
                if (
                    //TODO: Probably need better policy here, need to consider what
                    //TODO: it means for async operations ( single thread is used for many tasks)
                    //TODO: and for thread operations like indexing / replication that has just single
                    //TODO: usable thread. 
                    stack.Count < 4096)
                {
                    Context.Reset();
                    stack.Push(Context);
                }
                else
                {
                    Context.Dispose();
                }
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            foreach (var stack in _contextPool.Values)
            {
                while (stack.Count > 0)
                {
                    var ctx = stack.Pop();
                    ctx.Dispose();
                }
            }
            _contextPool.Dispose();
        }
    }
}