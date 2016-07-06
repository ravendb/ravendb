using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Blittable;
using SlowTests.Tests.Sorting;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var bulkInserts = new FastTests.Client.BulkInsert.BulkInserts())
            {
                bulkInserts.SimpleBulkInsertShouldWork().Wait();

            }
        }
    }
}