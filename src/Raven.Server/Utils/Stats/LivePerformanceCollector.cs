using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils.Stats
{
    public abstract class LivePerformanceCollector<T> : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private Task _task;

        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;

        protected LivePerformanceCollector(DocumentDatabase database)
        {
            Database = database;
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);

            _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);
        }

        protected void Start()
        {
            _task = Task.Run(StartCollectingStats);
        }

        public AsyncQueue<List<T>> Stats { get; } = new AsyncQueue<List<T>>();

        protected CancellationToken CancellationToken => _cts.Token;

        protected abstract Task StartCollectingStats();

        protected async Task RunInLoop()
        {
            while (CancellationToken.IsCancellationRequested == false)
            {
                await TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(3000), CancellationToken).ConfigureAwait(false);

                if (CancellationToken.IsCancellationRequested)
                    break;

                var performanceStats = PreparePerformanceStats();

                if (performanceStats.Count > 0)
                {
                    Stats.Enqueue(performanceStats);
                }
            }
        }

        protected abstract List<T> PreparePerformanceStats();

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
