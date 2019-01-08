using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils.Stats
{
    public abstract class LivePerformanceCollector<T> : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private Task _task;

        protected readonly Logger Logger;

        protected LivePerformanceCollector(CancellationToken parentCts, string loggingSource)
        {
            Logger = LoggingSource.Instance.GetLogger(loggingSource, GetType().FullName);

            _cts = CancellationTokenSource.CreateLinkedTokenSource(parentCts);
        }

        protected void Start()
        {
            _task = Task.Run(StartCollectingStats);
        }

        public AsyncQueue<List<T>> Stats { get; } = new AsyncQueue<List<T>>();

        protected CancellationToken CancellationToken => _cts.Token;

        protected virtual TimeSpan SleepTime => TimeSpan.FromSeconds(3);

        protected abstract Task StartCollectingStats();

        protected async Task RunInLoop()
        {
            while (CancellationToken.IsCancellationRequested == false)
            {
                await TimeoutManager.WaitFor(SleepTime, CancellationToken).ConfigureAwait(false);

                if (CancellationToken.IsCancellationRequested)
                    break;

                var performanceStats = PreparePerformanceStats();

                if (ShouldEnqueue(performanceStats))
                {
                    Stats.Enqueue(performanceStats);
                }
            }
        }

        protected virtual bool ShouldEnqueue(List<T> items)
        {
            return items.Count > 0;
        }

        protected abstract List<T> PreparePerformanceStats();

        public async Task<bool> SendStatsOrHeartbeatToWebSocket<TContext>(Task<WebSocketReceiveResult> receive, WebSocket webSocket, JsonContextPoolBase<TContext> contextPool, MemoryStream ms, int timeToWait) where TContext : JsonOperationContext
        {
            if (receive.IsCompleted || webSocket.State != WebSocketState.Open)
                return false;

            var tuple = await Stats.TryDequeueAsync(TimeSpan.FromMilliseconds(timeToWait));
            if (tuple.Item1 == false)
            {
                await webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, CancellationToken);
                return true;
            }

            ms.SetLength(0);

            using (contextPool.AllocateOperationContext(out TContext context))
            using (var writer = new AsyncBlittableJsonTextWriter(context, ms, CancellationToken))
            {
                WriteStats(tuple.Item2, writer, context);

                await writer.OuterFlushAsync();
            }

            ms.TryGetBuffer(out ArraySegment<byte> bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken);

            return true;
        }

        protected abstract void WriteStats(List<T> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context); 

        public virtual void Dispose()
        {
            if (CancellationToken.IsCancellationRequested)
                return;

            _cts.Cancel();

            var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}");

            exceptionAggregator.Execute(() =>
            {
                try
                {
                    _task.Wait();
                }
                catch (OperationCanceledException)
                {
                }
            });

            exceptionAggregator.Execute(() => _cts.Dispose());

            exceptionAggregator.ThrowIfNeeded();
        }

        protected class HandlerAndPerformanceStatsList<THandler, TStatsAggregator>
        {
            public readonly THandler Handler;

            public readonly BlockingCollection<TStatsAggregator> Performance;

            public HandlerAndPerformanceStatsList(THandler handler)
            {
                Handler = handler;
                Performance = new BlockingCollection<TStatsAggregator>();
            }
        }
    }
}
