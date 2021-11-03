using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;

namespace Subscriptions.Benchmark
{
    public class ConcurrentSubscriptionBenchmark : IDisposable
    {
        private int _batchSize;
        private string _subscriptionName;
        private readonly string _collectionName;
        private DocumentStore _store;

        public ConcurrentSubscriptionBenchmark(int batchSize,  string url,
            string databaseName = "freeDB", string collectionName = "Disks")
        {
            _batchSize = batchSize;

            _collectionName = collectionName;
            _store = new DocumentStore()
            {
                Database = databaseName,
                Urls = new []{ url },
                Conventions = new DocumentConventions()
            };
            _store.Initialize();
        }

        public class Thing
        {
            public string Name { get; set; }
        }

        public async Task PerformBenchmark(int workers, int fakeProcessingTimePerBatch, bool script, bool revision)
        {
            var runResult = await SingleTestRun(workers, fakeProcessingTimePerBatch, script, revision).ConfigureAwait(false);

            Console.WriteLine("Docs Processed: "+ runResult.DocsProccessed + " ,Docs Requested: " + runResult.DocsRequested + " Elapsed time: " + runResult.ElapsedMs);
        }

        private async Task<RunResult> SingleTestRun(int workers, int fakeProcessingTimePerBatch, bool script, bool revision = false)
        {
            try
            {
                if (string.IsNullOrEmpty(_subscriptionName))
                {
                    if (script == false && revision)
                    {
                        throw new InvalidOperationException("Can't have a revision without script");
                    }

                    SubscriptionCreationOptions subscriptionCreationParams;
                    string revisions = revision ? " (Revisions = true)" : "";
                    if (script)
                    {
                        subscriptionCreationParams = new SubscriptionCreationOptions
                        {
                            Query = "from " + _collectionName + revisions
                        };
                        _subscriptionName = await _store.Subscriptions.CreateAsync(subscriptionCreationParams).ConfigureAwait(false);
                        Console.WriteLine($"Created Subscription with query: '{subscriptionCreationParams.Query}'");
                    }
                    else
                    {
                        _subscriptionName =  await _store.Subscriptions.CreateAsync<Order>();
                        Console.WriteLine($"Created Subscription without query");
                    }
                }

                var workersList = new List<SubscriptionWorker<dynamic>>();
                var taskList = new List<Task>();
                Stopwatch sp = null;
                for (int i = 0; i < workers; i++)
                {
                    var worker = _store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(_subscriptionName)
                    {
                        Strategy = workers == 1 ? SubscriptionOpeningStrategy.OpenIfFree : SubscriptionOpeningStrategy.Concurrent,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                        MaxDocsPerBatch = _batchSize
                    });
                    workersList.Add(worker);
                    worker.OnEstablishedSubscriptionConnection += () =>
                    {
                        Interlocked.CompareExchange(ref sp, Stopwatch.StartNew(), null);
                    };
                }

                var tcs = new TaskCompletionSource<object>();
                var sentCount = 0;
                for (int i=0; i < workersList.Count; i++)
                {
                    taskList.Add(workersList[i].Run((async o =>
                    {
                        Console.WriteLine(sentCount);
                        if (Interlocked.Add(ref sentCount, o.Items.Count) >= 1_000_000)
                            tcs.TrySetResult(null);
                        await Task.Delay(fakeProcessingTimePerBatch);
                    })));
                }

                await tcs.Task.ConfigureAwait(false);

                foreach (var worker in workersList)
                {
                    await worker.DisposeAsync().ConfigureAwait(false);
                }
                
                await Task.WhenAll(taskList);
                
                return new RunResult
                {
                    DocsProccessed = sentCount,
                    DocsRequested = _batchSize,
                    ElapsedMs = sp.ElapsedMilliseconds
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        public class Order
        {
            private int Amount;
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
