using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client.Attachments;
using FastTests.Server.Documents.Expiration;
using FastTests.Server.Documents.Indexing.Static;
using FastTests.Smuggler;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using SlowTests.Client.Attachments;
using SlowTests.Server.Documents.Indexing;

namespace RavenDB4RCTests
{
    class Program
    {
        static void Main(string[] args)
        {
            {
                var sp = Stopwatch.StartNew();
                new ExpirationTests().CanAddEntityWithExpiry_BeforeActivatingExpirtaion_WillNotBeAbleToReadItAfterExpiry().Wait();
                Console.WriteLine(sp.Elapsed);
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
