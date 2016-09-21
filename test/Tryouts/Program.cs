using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Smuggler;
using SlowTests.Voron;
using Voron;

namespace Tryouts
{
    public class Program
    {
       
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting");
            var sp = Stopwatch.StartNew();
            using (var store = new DocumentStore
            {
                DefaultDatabase = "licensing",
                Url = "http://localhost:8080"
            })
            {
                store.Initialize();

                store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), @"C:\Users\ayende\Downloads\Dump of LicenseTracking, 2016-09-19 13-00.ravendbdump.gzip", CancellationToken.None)
                    .Wait();

            }
                Console.WriteLine(sp.Elapsed);
        }

    }
}
