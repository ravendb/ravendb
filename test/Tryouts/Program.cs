using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Smuggler;
using SlowTests.Smuggler;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var s = new FastTests.Smuggler.SmugglerApiTests())
            {
                s.CanExportAndImportWithVersioingRevisionDocuments().Wait();
            }

            //using (var x = new DocumentStore
            //{
            //    Url = "http://localhost:8080",
            //    DefaultDatabase = "licensing"
            //})
            //{
            //    x.Initialize();
            //    var sp = Stopwatch.StartNew();
            //    x.Smuggler.ImportAsync(new DatabaseSmugglerOptions(),
            //            @"C:\Users\ayende\Downloads\Dump of LicenseTracking, 2016-09-19 13-00.ravendbdump.gzip",
            //            CancellationToken.None)
            //        .Wait();

            //    Console.WriteLine(sp.Elapsed);
            //}
        }
    }
}

