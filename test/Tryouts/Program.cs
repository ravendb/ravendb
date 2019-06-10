using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using FastTests.Server.Documents;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using SlowTests.Cluster;
using SlowTests.Issues;
using Sparrow;
using StressTests.Server.Replication;
using Xunit.Sdk;

namespace Tryouts
{
    public class TestItem
    {
        public long A, B;
    }

    public static class Program
    {
        public static void Main(string[] args)
        {
            //using (var test = new TimeSeriesTests())
            //    test.CanStoreLargeNumberOfValues();

            var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Test"
            }.Initialize();


            var lines = File.ReadLines(@"D:\telemetryexport\telemetryexport.csv")
                .Select(line =>
                {
                    var parts = line.Split(',');
                    if (parts.Length != 4)
                        return default;
                    if (DateTime.TryParseExact(parts[0], "yyyy-MM-ddThh.mm.ssZ", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var date) == false)
                        return default;
                    if (double.TryParse(parts[2], out var a) == false)
                        return default;
                    if (double.TryParse(parts[3], out var b) == false)
                        return default;
                    var tag = parts[1];
                    return (date, tag, a, b);
                })
                .Where(x => x.date > DateTime.MinValue);
            var s = store.OpenSession();
            var ts = s.TimeSeriesFor("users/Jørgensen");
            var count = 0;
            var total = 0;
            foreach (var line in lines)
            {
                ts.Append("Telemetry", line.date, line.tag, new[] { line.a, line.b });
                total++;
                if(count++ > 1000)
                {
                    s.SaveChanges();
                    count = 0;
                    s.Dispose();
                    s = store.OpenSession();
                    ts = s.TimeSeriesFor("users/Jørgensen");
                    if(total % 100_000 == 0)
                        Console.WriteLine(total);
                }
            }

            s.SaveChanges();
        }


        private static async Task WriteMillionDocs(DocumentStore store)
        {
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 1_000_000; i++)
                {
                    var t = bulk.StoreAsync(new TestItem
                    {
                        A = i,
                        B = i
                    });
                    if (t.IsCompleted)
                        continue;
                    await t;
                }
            }
        }
    }
}
