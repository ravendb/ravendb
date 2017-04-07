using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Org.BouncyCastle.Asn1.Mozilla;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Tests.Infrastructure;
using Xunit;

namespace StressTests
{
    public class Databases : RavenTestBase
    {
        [Theory]
        [InlineData(150)]
        public void CanHandleMultipledatabasesOnWrite(int numberOfDatabases)
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore())
            {
                Console.WriteLine("Creating databases");
                CreateDatabases(numberOfDatabases, store);
                Console.WriteLine("Done creating databases");
                Product sampleProduct;
                using (var session = store.OpenSession(dbNumToDbName[0]))
                {
                    sampleProduct = session.Load<Product>("products/1");
                }
                Console.WriteLine("Starting load on the system");
                var timeToSpin = TimeSpan.FromMinutes(5);
                var minTimeBetweenIntervals = 500;
                StartAsyncQueryTask(store, numberOfDatabases, timeToSpin, minTimeBetweenIntervals);
                CreateLoadOnAllDatabases(numberOfDatabases, store, timeToSpin, minTimeBetweenIntervals, sampleProduct);                
                Console.WriteLine("Done with laod on the system");
                Console.WriteLine("Query statistics during the laod:");
                Console.WriteLine("Reported query times");
                Console.WriteLine($"Avarage={reportedAvarageQueryTime}ms Max={reportedMaxQueryTime}");
                Console.WriteLine("real query times (time on the client)");
                Console.WriteLine($"Avarage={avarageQueryTime}ms Max={maxQueryTime}");
                Console.WriteLine($"Got {totalNegativeDurations} negative query duration out of {totalQueryCount} total queries!");
            }
        }
        
        private static Dictionary<int,string> dbNumToDbName = new Dictionary<int, string>();
        private void StartAsyncQueryTask(DocumentStore store, int numberOfDatabases, TimeSpan timeToSpin, int minTimeBetweenIntervals)
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
                        using (var session = store.OpenSession(dbNumToDbName[dbNum]))
                        {
                            QueryStatistics queryStat;
                            sw.Restart();
                            session.Query<Product>().Statistics(out queryStat).Customize(x=>x.NoCaching()).Where(p => p.PricePerUnit > 1).Take(25).ToList();
                            var realQueryTime = sw.ElapsedMilliseconds;
                            //this is for no overflow
                            var onePart = (double)1 / (totalQueryCount + 1);
                            avarageQueryTime = onePart * totalQueryCount * avarageQueryTime + realQueryTime * onePart;


                            if (maxQueryTime < realQueryTime)
                            {
                                maxQueryTime = realQueryTime;
                            }
                            //Not sure if reported result is worth anything...
                            onePart = (double)1 / (totalQueryCount - totalNegativeDurations + 1);
                            if (queryStat.DurationMilliseconds >  0)
                            {
                                reportedAvarageQueryTime = onePart * (totalQueryCount - totalNegativeDurations) * reportedAvarageQueryTime + queryStat.DurationMilliseconds * onePart;
                                if (reportedMaxQueryTime < queryStat.DurationMilliseconds)
                                {
                                    reportedMaxQueryTime = queryStat.DurationMilliseconds;
                                }
                            }
                            else
                            {
                                totalNegativeDurations++;
                            }
                            
                            totalQueryCount++;
                        }

                    }
                    catch (ObjectDisposedException de)
                    {
                    }
                }
            }, cts.Token);
        }

        private static double avarageQueryTime;
        private static long totalQueryCount;
        private static long maxQueryTime;
        private static double reportedAvarageQueryTime;
        private static long reportedMaxQueryTime;
        private static int totalNegativeDurations;
        private void CreateLoadOnAllDatabases(int numberOfDatabases, DocumentStore store,TimeSpan timeToSpin,int minTimeBetweenIntervals, Product sampleProduct)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed< timeToSpin)
            {
                var startTime = sw.ElapsedMilliseconds;
                for (var i = 0; i < numberOfDatabases; i++)
                {
                    using (var session = store.OpenSession(dbNumToDbName[i]))
                    {
                        session.Store(sampleProduct,"products/");
                        session.SaveChanges();
                    }
                }
                var ranTime = sw.ElapsedMilliseconds - startTime;
                if (ranTime< minTimeBetweenIntervals)
                {
                    Thread.Sleep(minTimeBetweenIntervals - (int)ranTime);
                }
            }
            
        }

        private static void CreateDatabases(int numberOfDatabases, Raven.Client.Documents.DocumentStore store)
        {

            for (var i = 0; i < numberOfDatabases; i++)
            {

                var dbname = $"Northwind{i}";
                dbNumToDbName.Add(i,dbname);
                var doc = MultiDatabase.CreateDatabaseDocument(dbname);
                store.Admin.Server.Send(new CreateDatabaseOperation(doc));
                store.Admin.ForDatabase(dbname).Send(new CreateSampleDataOperation());
                Console.WriteLine($"Done creating {dbname}");
            }
            Console.WriteLine("Waiting for non stale last database");
            var statOperation = new GetStatisticsOperation();
            while (true)
            {
                var stat = store.Admin.ForDatabase(dbNumToDbName[numberOfDatabases - 1]).Send(statOperation);
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
