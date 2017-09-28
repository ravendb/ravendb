using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.Linq;

namespace RavenDB4RCTests
{
    class Program
    {
        static void Main(string[] args)
        {
            var documentStore = new DocumentStore
            {
                Urls = new[] { "http://4.live-test.ravendb.net" },
                Database = "TestStreamingTimeout"
            };

            documentStore.Initialize();

            if (ShouldInitData(documentStore))
            {
                InitializeData(documentStore);
            }

            using (var session = documentStore.OpenAsyncSession())
            {
                var query = session.Query<Doc>(collectionName: "Docs");
                var stream = session.Advanced.StreamAsync(query).Result;

                var position = 0;
                while (stream.MoveNextAsync().Result)
                {
                    ++position;
                    if (position % 1000 == 0)
                    {
                        Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Processing item " + position);
                    }
                }
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
