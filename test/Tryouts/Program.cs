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


            var lines = File.ReadAllLines(@"C:\Users\ayende\source\repos\ConsoleApp20\ConsoleApp20\bin\Debug\netcoreapp2.2\out.csv")
                .Select(line =>
                {
                    var parts = line.Split(',');
                    if (parts.Length != 2)
                        return default;
                    if (DateTime.TryParseExact(parts[0], "o", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var date) == false)
                        return default;
                    if (double.TryParse(parts[1], out var d) == false)
                        return default;

                    return (date, d);
                })
                .Where(x => x.date > DateTime.MinValue)
                .OrderBy(x => x.date)
                .ToList();
            var start = DateTime.MinValue;
            for (int i = 0; i < lines.Count; i += 250)
            {
                FlushBpm(store, lines, i, Math.Min(250, lines.Count - i));
                if ((lines[i].date - start).TotalDays > 30)
                {
                    start = lines[i].date;
                    Console.WriteLine(start);
                }
            }
        }

        private static void FlushBpm(IDocumentStore store, List<(DateTime, double)> list, int offset, int count)
        {
            using (var s = store.OpenSession())
            {
                var ts = s.TimeSeriesFor("users/oren");

                for (int i = 0; i < count; i++)
                {
                    var item = list[offset + i];
                    ts.Append("BPM", item.Item1, "watches/fitbit", new[] { item.Item2 });
                }

                s.SaveChanges();
            }
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
