using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Smuggler;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using FastTests.Issues;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.PeriodicExport;
using FastTests.Server.OAuth;
using FastTests.Server.Replication;
using FastTests.Sparrow;
using SlowTests.Issues;
using Sparrow;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var p = new SortingTests();
            p.DifferentSizesWithValues();
            p.DifferentSizes();
        }
    }
}