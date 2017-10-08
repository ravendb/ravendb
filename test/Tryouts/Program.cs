using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Blittable;

namespace RavenDB4RCTests
{
    class Program
    {
        static void Main(string[] args)
        {
            var originalSize = 90999;
            var bytes = new byte[originalSize];
            for (var i = 0; i < bytes.Length; i++)
            {
                bytes[i] = (byte)((i % 26) + 'a');
            }

            using (var stream = new MemoryStream())
            {
                stream.Write(bytes, 0, originalSize);
                stream.Flush();
                stream.Position = 0;
            }
            var t = new PeepingTomTest();
            t.PeepingTomStreamShouldPeepCorrectlyWithRandomValues(1291481720);

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
