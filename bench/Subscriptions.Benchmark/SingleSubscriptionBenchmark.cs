using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;

namespace Subscriptions.Benchmark
{
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
        private string _subscriptionId;
        private readonly string _collectionName;
        private DocumentStore _store;

        public SingleSubscriptionBenchmark(int batchSize,  string url,
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

        public async Task PerformBenchmark()
        {
            var runResult = await SingleTestRun().ConfigureAwait(false);

            Console.WriteLine(runResult.DocsProccessed + " " + runResult.DocsRequested + " " + runResult.ElapsedMs);
        }

        private async Task<RunResult> SingleTestRun()
        {
            try
            {
                if (string.IsNullOrEmpty(_subscriptionId))
                {
                    var subscriptionCreationParams = new SubscriptionCreationOptions
                    {
                        Criteria = new SubscriptionCriteria(_collectionName)
                    };
                    _subscriptionId = await _store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams).ConfigureAwait(false);
                }


                using (var subscription = _store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions(_subscriptionId)
                {
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                }))
                {
                    var tcs = new TaskCompletionSource<object>();
                    var sp = Stopwatch.StartNew();
                    int count = 0;
                    var task = subscription.Run(o =>
                    {
                        if (count++ >= _batchSize)
                            tcs.TrySetResult(null);
                    });;

                    await tcs.Task.ConfigureAwait(false);

                    await subscription.DisposeAsync().ConfigureAwait(false);

                    await task;
                    
                    return new RunResult
                    {
                        DocsProccessed = count,
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
