using System;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Indexes;

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

                var isStale = true;
                TimeSpan lastCheck = TimeSpan.Zero;

                while (isStale)
                {
                    Thread.Sleep(1000);

                    isStale = _store.DatabaseCommands.GetStatistics().StaleIndexes.Length != 0;

                    if (sw.Elapsed - lastCheck > TimeSpan.FromSeconds(1))
                    {
                        lastCheck = sw.Elapsed;

                        //var stats = _store.DatabaseCommands.GetIndexStatistics(ordersByCompany.IndexName);

                        //Console.WriteLine($"{nameof(stats.MapAttempts)}: {stats.MapAttempts} of {numberOfDocuments}");
                        //Console.WriteLine($"{nameof(stats.ReduceAttempts)}: {stats.ReduceAttempts}");
                    }
                }

                sw.Stop();

                Console.WriteLine($"Index became non-stale. It took {sw.Elapsed} to index ... docs"); // use stats
            }
        }
    }
}