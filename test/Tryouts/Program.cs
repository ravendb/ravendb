using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using NetTopologySuite.Utilities;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Data;
using Raven.Client.Document;

namespace Tryouts
{
    public class Program
    {
       
        class CustomType
        {
            public string Id { get; set; }
            public string Owner { get; set; }
            public int Value { get; set; }
            public List<string> Comments { get; set; }
            public DateTime Date { get; set; }
            public DateTimeOffset DateOffset { get; set; }
        }


        private const int numOfItems = 100;

        public static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8081",
                DefaultDatabase = "TestPatchPerformance"
            })
            {
                store.Initialize();

                /*using (var session = store.OpenSession())
                {
                    session.Store(new CustomType {Id = "Items/1", Value = 10, Comments = new List<string>(new[] {"one", "two", "three"})});
                    session.SaveChanges();
                }*/

                Console.Write("Start patching...");
                var sw = Stopwatch.StartNew();
                Parallel.For(0, 10000, new ParallelOptions
                {
                    MaxDegreeOfParallelism = 4
                }, i =>
                {
                    store.DatabaseCommands.Patch("Items/1", new PatchRequest
                    {
                        Script = @"this.Value = newVal",
                        Values =
                        {
                            ["newVal"] = 1
                        }
                    });
                });
                Console.WriteLine($"Elapsed : {sw.ElapsedMilliseconds} ms");

            }
        }
    }
}
