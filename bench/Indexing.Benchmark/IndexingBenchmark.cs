using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
        
        protected abstract AbstractIndexCreationTask[] Indexes { get; }

        public void Execute()
        {
            foreach (var index in Indexes)
            {
                Console.WriteLine($"Inserting {index.IndexName} index");

                index.Execute(_store);

                Console.WriteLine("waiting for results ...");

                var sw = Stopwatch.StartNew();

                var stalenessTimeout = TimeSpan.FromMinutes(5);
                QueryResult result = null;

#if !v35
                //Task.Factory.StartNew(() =>
                //{
                //    do
                //    {
                //        var stats = _store.DatabaseCommands.GetIndexStatistics(index.IndexName);

                //        Console.WriteLine($"{nameof(stats.MapAttempts)}: {stats.MapAttempts}");
                //        Console.WriteLine($"{nameof(stats.ReduceAttempts)}: {stats.ReduceAttempts}");

                //        Thread.Sleep(500);
                //    } while ((result != null && result.IsStale == false) || sw.Elapsed > stalenessTimeout);
                //}, TaskCreationOptions.LongRunning);

                result = _store.DatabaseCommands.Query(index.IndexName, new IndexQuery()
                {

                    WaitForNonStaleResultsTimeout = stalenessTimeout,

                    PageSize = 0,
                    Start = 0
                });

#else
                do
                {
                    result = _store.DatabaseCommands.Query(index.IndexName, new IndexQuery()
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

                Console.WriteLine($"It took {sw.Elapsed} to return non stale result");
            }
        }
    }
}