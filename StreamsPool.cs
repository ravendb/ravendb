using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;

namespace Raven.Munin
{
    public class StreamsPool
    {
        private readonly Func<Stream> createNewStream;
        private readonly ConcurrentDictionary<int, ConcurrentQueue<Stream>> openedStreamsPool = new ConcurrentDictionary<int, ConcurrentQueue<Stream>>();
        private int version;

        public StreamsPool(Func<Stream> createNewStream)
        {
            this.createNewStream = createNewStream;
        }

        public void Clear()
        {
            Stream result;
            var currentVersion = Interlocked.Increment(ref version);
            openedStreamsPool.TryAdd(currentVersion, new ConcurrentQueue<Stream>());
            ConcurrentQueue<Stream> value;
            if (openedStreamsPool.TryRemove(currentVersion, out value) == false)
                return;

            while(value.TryDequeue(out result))
            {
                result.Dispose();
            }
        }

        public IDisposable Use(out Stream stream)
        {
            var currentversion = Thread.VolatileRead(ref version);
            ConcurrentQueue<Stream> current;
            openedStreamsPool.TryGetValue(currentversion, out current);
            Stream value = current != null && current.TryDequeue(out value) ? 
                value : 
                createNewStream();
            stream = value;
            return new DisposableAction(delegate
            {
                ConcurrentQueue<Stream> current2;
                if (currentversion == Thread.VolatileRead(ref currentversion) && 
                    openedStreamsPool.TryGetValue(currentversion, out current2))
                {
                    current2.Enqueue(value);
                }
                else
                {
                    value.Dispose();
                }
            });
        }

        /// <summary>
        /// A helper class that translate between Disposable and Action
        /// </summary>
        public class DisposableAction : IDisposable
        {
            private readonly Action action;

            /// <summary>
            /// Initializes a new instance of the <see cref="DisposableAction"/> class.
            /// </summary>
            /// <param name="action">The action.</param>
            public DisposableAction(Action action)
            {
                this.action = action;
            }

            /// <summary>
            /// Execute the relevant actions
            /// </summary>
            public void Dispose()
            {
                action();
            }
        }
    }
}