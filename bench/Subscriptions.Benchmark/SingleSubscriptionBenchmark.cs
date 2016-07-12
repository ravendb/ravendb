using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
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
        private int? _batchSize;
        private DocumentStore _store;

        public SingleSubscriptionBenchmark(string url = "http://localhost:8080", int? batchSize = null, string databaseName = "freeDB")
        {
            _batchSize = batchSize;
            _store = new DocumentStore()
            {
                DefaultDatabase = databaseName,
                Url = url
            };
            _store.Initialize();

        }

        public class Thing
        {
            public string Name { get; set; }
        }
        public void PerformBenchmark()
        {
            var runResult = SingleTestRun(100 * 1000).Result;
            Console.WriteLine(runResult.DocsProccessed + " " + runResult.DocsRequested + " " + runResult.ElapsedMs);
        }


        private async Task<RunResult> SingleTestRun(int docCount = 10000, string collectionName="Disks")
        {

            var subscriptionId = await _store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria()
            {
                Collection = collectionName
            });
            using (var subscription = _store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions()
            {
                MaxDocsPerBatch = _batchSize ?? docCount,
                SubscriptionId = subscriptionId
            }))
            {
                var observer = new CounterObserver(docCount);
                var sp = Stopwatch.StartNew();
                subscription.Subscribe(observer);
                await subscription.StartAsync();

                await observer.Tcs.Task;

                await subscription.DisposeAsync();
                return new RunResult
                {
                    DocsProccessed = observer.CurCount,
                    DocsRequested = docCount,
                    ElapsedMs = sp.ElapsedMilliseconds
                };
            }
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
