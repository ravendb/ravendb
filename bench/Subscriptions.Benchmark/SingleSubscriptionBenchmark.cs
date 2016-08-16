using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace SubscriptionsBenchmark
{

    public class CounterObserver : IObserver<RavenJObject>
    {
        public int MaxCount { get; private set; }
        public int CurCount { get; private set; }

        public readonly TaskCompletionSource<bool> Tcs = new TaskCompletionSource<bool>();

        public CounterObserver(int maxCount)
        {
            MaxCount = maxCount;
            CurCount = 0;
        }
        public void OnCompleted()
        {
            if (Tcs.Task.IsCompleted)
                return;
            if (CurCount != MaxCount)
            {
                Task.Run(() => Tcs.SetResult(false));

            }
            else
            {
                Task.Run(() => Tcs.SetResult(true));
            }
        }

        public void OnError(Exception error)
        {
            //if (string.Compare(error.Message, "Stream was not writable.", StringComparison.Ordinal)!= 0)
                Console.WriteLine(error);
        }

        public void OnNext(RavenJObject value)
        {
            if (Tcs.Task.IsCompleted)
                return;
            CurCount++;

            if (CurCount == MaxCount)
                Tcs.SetResult(true);

        }
    }

    public class RunResult
    {
        public int DocsRequested { get; set; }
        public long ElapsedMs { get; set; }
        public long DocsProccessed { get; set; }

        public override string ToString()
        {
            return $"Elapsed: {ElapsedMs}; MaxDocs: {DocsRequested}; ProccessedDocs: {DocsProccessed}";
        }
    }
    public class SingleSubscriptionBenchmark : IDisposable
    {
        private int _batchSize;
        private long? _subscriptionId;
        private readonly string _collectionName;
        private DocumentStore _store;

        public SingleSubscriptionBenchmark(int batchSize , string url, 
            string databaseName = "freeDB", string collectionName = "Disks")
        {
            _batchSize = batchSize;
            
            _collectionName = collectionName;
            _store = new DocumentStore()
            {
                DefaultDatabase = databaseName,
                Url = url,
                Conventions = new DocumentConvention()
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately
                }
            };
            _store.Initialize();

        }

        public class Thing
        {
            public string Name { get; set; }
        }

        public async Task PerformBenchmark()
        {
            var runResult = await SingleTestRun().ConfigureAwait(false);
            
            Console.WriteLine(runResult.DocsProccessed + " " + runResult.DocsRequested + " " + runResult.ElapsedMs);
        }

        private async Task<RunResult> SingleTestRun()
        {
            try
            {
                if (_subscriptionId.HasValue == false)
                {
                    _subscriptionId = await _store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria()
                    {
                        Collection = _collectionName
                    }).ConfigureAwait(false);
                }
            
            
                using (var subscription = _store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions()
                {
                    MaxDocsPerBatch = 20,
                    SubscriptionId = _subscriptionId.Value,
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                }))
                {
                    var observer = new CounterObserver(_batchSize);
                    var sp = Stopwatch.StartNew();
                    subscription.Subscribe(observer);
                    await subscription.StartAsync().ConfigureAwait(false);

                    await observer.Tcs.Task.ConfigureAwait(false);

                    await subscription.DisposeAsync().ConfigureAwait(false);
                    return new RunResult
                    {
                        DocsProccessed = observer.CurCount,
                        DocsRequested = _batchSize,
                        ElapsedMs = sp.ElapsedMilliseconds
                    };
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
