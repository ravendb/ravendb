using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Server.Documents.Indexing.Auto;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using SlowTests.Tests.Sorting;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            Parallel.For(0, 1000, i =>
            {
                Console.WriteLine(i);
                using (var n = new SlowTests.Voron.RecoveryMultipleJournals())
                {
                    n.CorruptingOneTransactionWillKillAllFutureTransactions();
                }
            });
        }
    }
}