using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Client;
using Raven.Client.Indexes;
#if v35
using Raven.Abstractions.Data;
#else
using Raven.Client.Data;
using Raven.Client.Data.Queries;
#endif

namespace Indexing.Benchmark
{
    public abstract class IndexingBenchmark
    {
        private readonly IDocumentStore _store;

        public IndexingBenchmark(IDocumentStore store)
        {
            _store = store;
        }
        
        public abstract IndexingTestRun[] IndexTestRuns { get; }
        
        public void Execute()
        {
            foreach (var test in IndexTestRuns)
            {
                test.Index.Execute(_store);
                
                Console.WriteLine($"{Environment.NewLine}{test.Index.IndexName} index created");

                Console.WriteLine("waiting for non-stale results ...");

                var sw = Stopwatch.StartNew();

                var stalenessTimeout = TimeSpan.FromMinutes(5);
                QueryResult result = null;

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

                result = _store.DatabaseCommands.Query(test.Index.IndexName, new IndexQuery()
                {
                    WaitForNonStaleResultsTimeout = stalenessTimeout,
                    PageSize = 0,
                    Start = 0
                });

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

                Console.WriteLine($"It took {sw.Elapsed} to return a non stale result. {(double)test.NumberOfRelevantDocs / sw.Elapsed.Seconds:#,#} docs / sec indexed");
            }
        }
    }
}