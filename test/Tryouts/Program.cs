using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using FastTests.Server.Documents.Replication;
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
            for (int i = 0; i < 1000; i++)
            {
                using (var x = new AutomaticConflictResolution())
                {
                    x.Resolve_to_latest_version_tombstone_is_latest_the_incoming_document_is_replicated();
                }
                Console.WriteLine(i + 1);
            }
            //Console.WriteLine("Starting");
            //var sp = Stopwatch.StartNew();
            //using (var store = new DocumentStore
            //{
            //    DefaultDatabase = "licensing",
            //    Url = "http://localhost:8080"
            //})
            //{
            //    store.Initialize();

            //    store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), @"C:\Users\ayende\Downloads\Dump of LicenseTracking, 2016-09-19 13-00.ravendbdump.gzip", CancellationToken.None)
            //        .Wait();

            //}
            //    Console.WriteLine(sp.Elapsed);
        }

    }
}
