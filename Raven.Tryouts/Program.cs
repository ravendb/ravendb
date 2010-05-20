using System;
using System.Linq;
using Raven.Client.Document;
using System.IO;
using System.Diagnostics;

namespace RavenTestbed
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Directory.Exists("ravendb")) Directory.Delete("ravendb", true);
            var documentStore = new DocumentStore
            {
                Configuration =
                {
                    DataDirectory = "ravendb"
                }
            };
            documentStore.Initialise();
            documentStore.DatabaseCommands.PutIndex("FooByName",
                new IndexDefinition<Foo>()
                {
                    Map = docs => from doc in docs select new { Name = doc.Name }
                });

            var numberOfFoos = 10000;

            // Insert Foos
            using (var session = documentStore.OpenSession())
            {
                for (var i = 0; i < numberOfFoos; i++)
                {
                    var newFoo = new Foo { Name = i.ToString() };
                    session.Store(newFoo);
                }
                session.SaveChanges();
            }

            // Query a single foo to wait for the index
            using (var session = documentStore.OpenSession())
            {
                var foo = session.LuceneQuery<Foo>("FooByName").Where("Name:1").WaitForNonStaleResults(TimeSpan.FromMinutes(1)).First();
            }
            Console.WriteLine("starting querying");
            // Query all foos twice
            for (var queryRun = 0; queryRun < 150; queryRun++)
            {
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                for (var i = 0; i < numberOfFoos; i++)
                {
                    using (var session = documentStore.OpenSession())
                    {
                        var foo = session.LuceneQuery<Foo>("FooByName").Where("Name:" + i.ToString()).First();
                    }
                }
                stopWatch.Stop();
                Console.WriteLine("{0}. run: Querying {1} Foos in {2} ({3}ms per query)", queryRun, numberOfFoos, stopWatch.Elapsed, stopWatch.ElapsedMilliseconds / numberOfFoos);
            }
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}