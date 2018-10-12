using System;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Indexing.Benchmark
{
    public abstract class IndexingBenchmark
    {
        private readonly IDocumentStore _store;

        protected IndexingBenchmark(IDocumentStore store)
        {
            _store = store;
        }

        public abstract IndexingTestRun[] IndexTestRuns { get; }

        public void Execute()
        {
            foreach (var test in IndexTestRuns)
            {
                test.Index.Execute(_store);

                Console.WriteLine($"{Environment.NewLine}{test.Index.IndexName} index created. Waiting for non-stale results ...");

                var sw = Stopwatch.StartNew();

                var stalenessTimeout = TimeSpan.FromMinutes(15);
                QueryResult result;

#if !v35
                //Task.Factory.StartNew(() =>
                //{
                //    do
                //    {
                //        var stats = _store.DatabaseCommands.GetIndexStatistics(test.Index.IndexName);

                //        Console.WriteLine($"{nameof(stats.MapAttempts)}: {stats.MapAttempts}");
                //        Console.WriteLine($"{nameof(stats.ReduceAttempts)}: {stats.ReduceAttempts}");

                //        Thread.Sleep(500);
                //    } while ((result != null && result.IsStale == false) || sw.Elapsed > stalenessTimeout);
                //}, TaskCreationOptions.LongRunning);

                var requestExecuter = _store.GetRequestExecutor();
                JsonOperationContext context;
                using(var session = _store.OpenSession())
                using (requestExecuter.ContextPool.AllocateOperationContext(out context))
                {
                    do
                    {
                        var queryCommand = new QueryCommand((InMemoryDocumentSessionOperations)session, new IndexQuery
                        {
                            Query = $"FROM INDEX '{test.Index.IndexName}' OFFSET 0 FETCH 0"
                        });

                        requestExecuter.Execute(queryCommand, context);

                        result = queryCommand.Result;

                        Thread.Sleep(100);
                    } while (result.IsStale || sw.Elapsed > stalenessTimeout);
                }
#else
                do
                {
                    result = _store.DatabaseCommands.Query(test.Index.IndexName, new IndexQuery()
                    {
                        PageSize = 0,
                        Start = 0
                    });

                    Thread.Sleep(100);
                } while (result.IsStale || sw.Elapsed > stalenessTimeout);
#endif

                if (result.IsStale)
                {
                    throw new InvalidOperationException($"Index is stale after {stalenessTimeout}");
                }

                sw.Stop();

                Console.WriteLine($"It took {sw.Elapsed} to return a non stale result. {test.NumberOfRelevantDocs / sw.Elapsed.TotalSeconds:#,#} docs / sec indexed");
            }
        }
    }
}
