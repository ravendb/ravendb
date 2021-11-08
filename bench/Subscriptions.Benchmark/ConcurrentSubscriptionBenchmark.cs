using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Subscriptions;

namespace Subscriptions.Benchmark
{
    public class ConcurrentSubscriptionBenchmark : IDisposable
    {
        private readonly int _batchSize;
        private string _subscriptionName;
        private readonly string _collectionName;
        private readonly DocumentStore _store;
        private bool _revisionsEnabled;
        private readonly int _docsAmountToTest;

        public ConcurrentSubscriptionBenchmark(int batchSize,  string url, int docsAmountToTest,
            string databaseName = "freeDB", string collectionName = "Disks")
        {
            _batchSize = batchSize;
            _collectionName = collectionName;
            _revisionsEnabled = false;
            _docsAmountToTest = docsAmountToTest;
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

        public void GenerateDocumentsAndRevisions(int amount)
        {
            EnableRevisions();

            Console.WriteLine($"Generating {amount} documents..");
            //generate documents
            using (BulkInsertOperation bulkInsert = _store.BulkInsert())
            {
                for (int i = 0; i <= amount; i++)
                {
                    bulkInsert.Store(new Order { Amount = 1 });
                }
            }
        }

        private async Task<RunResult> SingleTestRun(int workers, int fakeProcessingTimePerBatch, bool script, bool revision = false)
        {
            try
            {
                if (script == false && revision)
                {
                    throw new InvalidOperationException("Can't have a revision without script");
                }

                string revisions = revision ? " (Revisions = true)" : "";
                if (script)
                {
                    SubscriptionCreationOptions subscriptionCreationParams = new SubscriptionCreationOptions
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

                EnableRevisions();

                var workersList = new List<SubscriptionWorker<dynamic>>();
                var taskList = new List<Task>();
                Stopwatch sp = null;
                for (int i = 0; i < workers; i++)
                {
                    var worker = _store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(_subscriptionName)
                    {
                        Strategy = SubscriptionOpeningStrategy.Concurrent,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                        MaxDocsPerBatch = _batchSize
                    });
                    workersList.Add(worker);
                    worker.OnEstablishedSubscriptionConnection += () =>
                    {
                        Interlocked.CompareExchange(ref sp, Stopwatch.StartNew(), null);
                    };
                }

                var tcs = new TaskCompletionSource<RunResult>();
                var sentCount = 0;
                foreach (var worker in workersList)
                {
                    taskList.Add(worker.Run((async o =>
                    {
                        if (Interlocked.Add(ref sentCount, o.Items.Count) >= _docsAmountToTest)
                            tcs.TrySetResult(new RunResult
                            {
                                DocsProccessed = sentCount,
                                DocsRequested = _batchSize,
                                ElapsedMs = sp.ElapsedMilliseconds
                            });
                        await Task.Delay(fakeProcessingTimePerBatch);
                    })));
                }

                var result = await tcs.Task.ConfigureAwait(false);

                foreach (var worker in workersList)
                {
                    await worker.DisposeAsync().ConfigureAwait(false);
                }
                
                await Task.WhenAll(taskList);
                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        private void EnableRevisions()
        {
            if (_revisionsEnabled == false)
            {
                //enable revisions
                _store.Maintenance.Send(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true,
                        MinimumRevisionsToKeep = 1,
                        MinimumRevisionAgeToKeep = TimeSpan.FromDays(14),
                    }
                }));
                _revisionsEnabled = true;
            }
        }

        public void DeleteDocuments()
        {
            var operation = _store
                .Operations
                .Send(new DeleteByQueryOperation(new IndexQuery
                {
                    Query = "from " + _collectionName
                }));

            operation.WaitForCompletion(TimeSpan.FromSeconds(15));
        }

        public class Order
        {
            public int Amount;
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
