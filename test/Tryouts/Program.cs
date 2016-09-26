using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using FastTests.Server.Documents.Replication;
using FastTests.Voron.Storage;
using Raven.Client.Document;
using Raven.Client.Smuggler;
using SlowTests.SlowTests.Bugs;
using SlowTests.Voron;
using Voron;

namespace Tryouts
{
    public class Program
    {
       
        public static void Main(string[] args)
        {
            //using (var x = new FastTests.Server.OAuth.CanAuthenticate())
            //{
            //    x.CanStoreAndDeleteApiKeys();
            //    if (DateTime.Now.Year == 2016)
            //        return;
            //}
            Console.WriteLine("Starting");
            using (var store = new DocumentStore
            {
                DefaultDatabase = "licensing",
                Url = "http://localhost:8080"
            })
            {
                store.Initialize();

                var sp = Stopwatch.StartNew();
                store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), @"C:\Users\ayende\Downloads\Dump of LicenseTracking, 2016-09-22 15-51.ravendbdump.gzip", CancellationToken.None)
                    .Wait();


                Console.WriteLine("Inserted in " + sp.Elapsed);
                sp.Restart();
                var done = new HashSet<string>();
                while (true)
                {
                    bool all = true;
                    foreach (var index in store.DatabaseCommands.GetStatistics().Indexes)
                    {
                        if (index.IsStale)
                        {
                            all = false;
                        }
                        else if (done.Add(index.Name))
                        {
                            Console.WriteLine(index.Name + " done in " + sp.Elapsed);
                        }
                    }
                    if (all)
                        break;
                    Thread.Sleep(100);
                }
                Console.WriteLine("Indexed in " + sp.Elapsed);
            }
        }

    }
}
