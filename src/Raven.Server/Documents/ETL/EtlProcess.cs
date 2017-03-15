using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlProcess<TExtracted, TTransformed> : IDisposable where TExtracted : ExtractedItem
    {
        private readonly ManualResetEventSlim _waitForChanges = new ManualResetEventSlim();
        private readonly EtlConfiguration _configuration;
        private readonly CancellationTokenSource _cts;
        private Thread _thread;
        protected readonly Logger Logger;
        protected readonly DocumentDatabase Database;
        public readonly EtlStatistics Statistics;
        public readonly string Tag;

        protected EtlProcess(DocumentDatabase database, EtlConfiguration configuration, string tag)
        {
            _configuration = configuration;
            Tag = tag;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(database.DatabaseShutdown);

            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
            Database = database;
            Statistics = new EtlStatistics(tag, _configuration.Name, Database.NotificationCenter);
        }

        protected CancellationToken CancellationToken => _cts.Token;

        public string Name => _configuration.Name;

        protected abstract IEnumerator<TExtracted> ConvertDocsEnumerator(IEnumerator<Document> docs);

        protected abstract IEnumerator<TExtracted> ConvertTombstonesEnumerator(IEnumerator<DocumentTombstone> tombstones);

        public virtual IEnumerable<TExtracted> Extract(DocumentsOperationContext context)
        {
            var documents = Database.DocumentsStorage.GetDocumentsFrom(context, _configuration.Collection, Statistics.LastProcessedEtag + 1, 0, int.MaxValue);
            var tombstones = Database.DocumentsStorage.GetTombstonesFrom(context, _configuration.Collection, Statistics.LastProcessedEtag + 1, 0, int.MaxValue);

            using (var documentsIt = documents.GetEnumerator())
            using (var tombstonesIt = tombstones.GetEnumerator())
            {
                using (var merged = new MergedEnumerator<TExtracted>())
                {
                    merged.AddEnumerator(ConvertDocsEnumerator(documentsIt));
                    merged.AddEnumerator(ConvertTombstonesEnumerator(tombstonesIt));

                    while (merged.MoveNext())
                    {
                        yield return merged.Current;
                    }
                }
            }
        }

        public abstract IEnumerable<TTransformed> Transform(IEnumerable<TExtracted> items, DocumentsOperationContext context);

        public abstract bool Load(IEnumerable<TTransformed> items);

        public abstract bool CanContinueBatch();

        protected abstract void LoadLastProcessedEtag(DocumentsOperationContext context);

        protected abstract void StoreLastProcessedEtag(DocumentsOperationContext context);

        public void NotifyAboutWork()
        {
            _waitForChanges.Set();
        }

        public void Start()
        {
            if (_thread != null)
                return;

            if (_configuration.Disabled)
                return;

            _thread = new Thread(() =>
            {
                // This has lower priority than request processing, so we let the OS
                // schedule this appropriately
                Threading.TrySettingCurrentThreadPriority(ThreadPriority.BelowNormal);

                Run();
            })
            {
                Name = $"{Tag} process: {Name}",
                IsBackground = true
            };

            if (Logger.IsInfoEnabled)
                Logger.Info($"Starting {Tag} process: '{Name}'.");

            _thread.Start();
        }

        public void Stop()
        {
            if (_thread == null)
                return;

            if (Logger.IsInfoEnabled)
                Logger.Info($"Stopping {Tag} process: '{Name}'.");

            _cts.Cancel();

            var thread = _thread;
            _thread = null;

            if (Thread.CurrentThread != thread) // prevent a deadlock
                thread.Join();
        }

        public void Run()
        {
            while (CancellationToken.IsCancellationRequested == false)
            {
                _waitForChanges.Reset();

                var startTime = Database.Time.GetUtcNow();
                var duration = Stopwatch.StartNew();


                // TODO arek
                //if (Statistics.SuspendUntil.HasValue && Statistics.SuspendUntil.Value > Database.Time.GetUtcNow())
                //    return;

                var didWork = false;

                try
                {
                    DocumentsOperationContext context;
                    using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                    {
                        using (context.OpenReadTransaction())
                        {
                            LoadLastProcessedEtag(context);

                            var extracted = Extract(context);

                            var transformed = Transform(extracted, context);

                            didWork = Load(transformed);
                        }

                        if (didWork)
                        {
                            using (var tx = context.OpenWriteTransaction())
                            {
                                StoreLastProcessedEtag(context);
                                tx.Commit();
                            }

                            continue;
                        }
                    }

                    
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                finally
                {
                    duration.Stop();

                    // TODO arek
                    //MetricsCountersManager.BatchSizeMeter.Mark(countOfReplicatedItems);
                    //MetricsCountersManager.UpdateReplicationPerformance(new SqlEtlPerformanceStats
                    //{
                    //    BatchSize = countOfReplicatedItems,
                    //    Duration = spRepTime.Elapsed,
                    //    Started = startTime
                    //});
                    
                    // TODO arek

                    if (didWork && CancellationToken.IsCancellationRequested == false)
                    {
                        var afterReplicationCompleted = Database.SqlReplicationLoader.AfterReplicationCompleted;
                        afterReplicationCompleted?.Invoke(Statistics);
                    }
                }

                try
                {
                    _waitForChanges.Wait(CancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        public void Dispose()
        {
            if (CancellationToken.IsCancellationRequested)
                return;

            var exceptionAggregator = new ExceptionAggregator(Logger, $"Could not dispose {GetType().Name}: '{Name}'");

            exceptionAggregator.Execute(Stop);
            exceptionAggregator.Execute(() => _cts.Dispose());
            exceptionAggregator.Execute(() => _waitForChanges.Dispose());
            
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}