using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Issues;
using FastTests.Server.NotificationCenter;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using SlowTests.Voron.Issues;

/*
    Code reference - please DO NOT REMOVE:
         
    DebuggerAttachedTimeout.DisableLongTimespan = true;
    
    Console.WriteLine(Process.GetCurrentProcess().Id);
    Console.WriteLine();
    
    LoggingSource.Instance.SetupLogMode(LogMode.Information, @"c:\work\ravendb\logs");
 */

namespace Tryouts
{
    class Program
    {
        static void Main(string[] args)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            using (var documentStore = new DocumentStore
            {
                Database = "Northwind",
                Urls = new []{ "http://127.0.0.1:8080" }               
            })
            {
                documentStore.Initialize();
                long x = 0;
                var mre = new ManualResetEventSlim();
                Parallel.For(0, 10, i =>
                {
                    while (mre.IsSet == false)
                    {

                        var databaseRecord = documentStore.Admin.Server.Send(new GetDatabaseRecordOperation("test"));
                        if (databaseRecord == null)
                        {
                            documentStore.Admin.Server.Send(new CreateDatabaseOperation(new DatabaseRecord("test")));
                        }
                    
                        Console.Write(Interlocked.Increment(ref x));
                    }
                });

                Console.ReadKey();                
                mre.Set();
            }


        }

        static async Task MainAsync()
        {
            var documentStore = new DocumentStore
            {
                Urls = new[] {"http://4.live-test.ravendb.net"},
                Database = "Test"
            };
         
            documentStore.Initialize();

            while (true)
            {
                using (var s = documentStore.OpenAsyncSession())
                {
                    dynamic load;
                    using (documentStore.AggressivelyCache())
                    {
                        load = await s.LoadAsync<dynamic>("users/1");
                    }
                    Console.WriteLine(load.Name);
                    Console.WriteLine(documentStore.GetRequestExecutor().NumberOfServerRequests);
                }
                Console.ReadLine();
            }
        }

        private static bool ShouldInitData(DocumentStore documentStore)
        {
            using (var session = documentStore.OpenSession())
            {
                var doc = session.Load<Doc>("doc/1");
                return doc == null;
            }
        }

        private static void InitializeData(DocumentStore documentStore)
        {
            Console.WriteLine("Generating data.");
            var rng = new Random();
            for (int batchNo = 0; batchNo < 100; batchNo++)
            {
                Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Generating batch " + batchNo);
                using (var session = documentStore.OpenSession())
                {
                    for (int i = 1; i <= 1000; i++)
                    {
                        session.Store(new Doc
                        {
                            Id = "doc/" + (batchNo * 1000 + i),
                            NumVals = Enumerable.Range(1, 300).ToDictionary(x => "Value-" + x, _ => rng.NextDouble()),
                        });
                    }
                    session.SaveChanges();
                }
            }
            Console.WriteLine("Data generated.");
        }
    }

    public class Doc
    {
        public string Id { get; set; }
        public Dictionary<string, double> NumVals { get; set; }
    }
}
