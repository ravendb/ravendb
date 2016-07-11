using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using SlowTests.Tests.Sorting;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine( i);
                using (var n = new Incremental())
                {
                    n.IncrementalBackupShouldCopyJustNewPagesSinceLastBackup();
                }
            }
        }
    }
}