using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;

namespace StressTests
{
    public class Databases : RavenTestBase
    {
        [NightlyBuildFact64Bit]
        public void CanHandleMultipleDatabasesOnWrite()
        {
            RunTest(numberOfDatabases: 25);
        }

        [NightlyBuildFact32Bit]
        public void CanHandleMultipleDatabasesOnWrite32()
        {
            RunTest(numberOfDatabases: 10);
        }

        private void RunTest(int numberOfDatabases)
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore())
            {
                Console.WriteLine("Creating databases");
                CreateDatabases(numberOfDatabases, store);
                Console.WriteLine("Done creating databases");
                Product sampleProduct;
                using (var session = store.OpenSession(DbNumToDbName[0]))
                {
                    sampleProduct = session.Load<Product>("products/1-A");
                }

                Console.WriteLine("Starting load on the system");
                var timeToSpin = TimeSpan.FromMinutes(5);
                var minTimeBetweenIntervals = 500;
                StartAsyncQueryTask(store, numberOfDatabases, timeToSpin, minTimeBetweenIntervals);
                CreateLoadOnAllDatabases(numberOfDatabases, store, timeToSpin, minTimeBetweenIntervals, sampleProduct);
                Console.WriteLine("Done with laod on the system");
                Console.WriteLine("Query statistics during the laod:");
                Console.WriteLine("Reported query times");
                Console.WriteLine($"Avarage={_reportedAvarageQueryTime}ms Max={_reportedMaxQueryTime}");
                Console.WriteLine("real query times (time on the client)");
                Console.WriteLine($"Avarage={_avarageQueryTime}ms Max={_maxQueryTime}");
                Console.WriteLine($"Results came from cache {_totalQueryUsedCachedResults} times out of {_totalQueryCount} total queries.");
            }
        }


        private readonly Dictionary<int, string> DbNumToDbName = new Dictionary<int, string>();

        private void StartAsyncQueryTask(IDocumentStore store, int numberOfDatabases, TimeSpan timeToSpin, int minTimeBetweenIntervals)
        {
            var cts = new CancellationTokenSource();
            var rand = new Random(numberOfDatabases);

            Task.Run(() =>
            {
                var sw = new Stopwatch();
                while (true)
                {
                    var dbNum = rand.Next(numberOfDatabases);

                    try
                    {
                        using (var session = store.OpenSession(DbNumToDbName[dbNum]))
                        {
                            QueryStatistics queryStat;
                            sw.Restart();
                            session.Query<Product>().Statistics(out queryStat).Customize(x => x.NoCaching()).Where(p => p.PricePerUnit > 1).Take(25).ToList();
                            var realQueryTime = sw.ElapsedMilliseconds;
                            //this is for no overflow
                            var onePart = (double)1 / (_totalQueryCount + 1);
                            _avarageQueryTime = onePart * _totalQueryCount * _avarageQueryTime + realQueryTime * onePart;


                            if (_maxQueryTime < realQueryTime)
                            {
                                _maxQueryTime = realQueryTime;
                            }
                            //Not sure if reported result is worth anything...
                            onePart = (double)1 / (_totalQueryCount - _totalQueryUsedCachedResults + 1);
                            if (queryStat.DurationInMs > 0)
                            {
                                _reportedAvarageQueryTime = onePart * (_totalQueryCount - _totalQueryUsedCachedResults) * _reportedAvarageQueryTime + queryStat.DurationInMs * onePart;
                                if (_reportedMaxQueryTime < queryStat.DurationInMs)
                                {
                                    _reportedMaxQueryTime = queryStat.DurationInMs;
                                }
                            }
                            else
                            {
                                _totalQueryUsedCachedResults++;
                            }

                            _totalQueryCount++;
                        }

                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
            }, cts.Token);
        }

        private static double _avarageQueryTime;
        private static long _totalQueryCount;
        private static long _maxQueryTime;
        private static double _reportedAvarageQueryTime;
        private static long _reportedMaxQueryTime;
        private static int _totalQueryUsedCachedResults;

        private void CreateLoadOnAllDatabases(int numberOfDatabases, IDocumentStore store, TimeSpan timeToSpin, int minTimeBetweenIntervals, Product sampleProduct)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeToSpin)
            {
                var startTime = sw.ElapsedMilliseconds;
                for (var i = 0; i < numberOfDatabases; i++)
                {
                    using (var session = store.OpenSession(DbNumToDbName[i]))
                    {
                        session.Store(sampleProduct, "products/");
                        session.SaveChanges();
                    }
                }
                var ranTime = sw.ElapsedMilliseconds - startTime;
                if (ranTime < minTimeBetweenIntervals)
                {
                    Thread.Sleep(minTimeBetweenIntervals - (int)ranTime);
                }
            }

        }

        private void CreateDatabases(int numberOfDatabases, Raven.Client.Documents.DocumentStore store)
        {

            for (var i = 0; i < numberOfDatabases; i++)
            {

                var dbname = $"Northwind{i}";
                DbNumToDbName.Add(i, dbname);
                var doc = new DatabaseRecord(dbname);
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));
                store.Maintenance.ForDatabase(dbname).Send(new CreateSampleDataOperation());
                Console.WriteLine($"Done creating {dbname}");
            }
            Console.WriteLine("Waiting for non stale last database");
            var statOperation = new GetStatisticsOperation();
            while (true)
            {
                var stat = store.Maintenance.ForDatabase(DbNumToDbName[numberOfDatabases - 1]).Send(statOperation);
                if (stat.StaleIndexes.Any())
                    Thread.Sleep(500);
                else
                {
                    Console.WriteLine("Last databse is not stale assuming all database are ready.");
                    return;
                }
            }
        }
    }
}
