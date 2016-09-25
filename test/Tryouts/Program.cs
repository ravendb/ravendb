using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
            using (var x = new FastTests.Voron.Tables.SecondayIndex())
            {
                x.CanInsertThenReadBySecondary();
            }
            //Console.WriteLine("Starting");
            //using (var store = new DocumentStore
            //{
            //    DefaultDatabase = "licensing",
            //    Url = "http://localhost:8080"
            //})
            //{
            //    store.Initialize();

            //    var sp = Stopwatch.StartNew();
            //    store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), @"C:\Users\ayende\Downloads\Dump of LicenseTracking, 2016-09-19 13-00.ravendbdump.gzip", CancellationToken.None)
            //        .Wait();


            //    Console.WriteLine("Inserted in " + sp.Elapsed);
            //    sp.Restart();
            //    while (true)
            //    {
            //        if (store.DatabaseCommands.GetStatistics().Indexes.All(x => x.IsStale == false))
            //        {
            //            break;
            //        }
            //        Thread.Sleep(100);
            //    }
            //    Console.WriteLine("Indexed in " + sp.Elapsed);

            //}
        }

    }
}
