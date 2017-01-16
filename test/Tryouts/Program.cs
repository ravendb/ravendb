using System;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Server.Documents.Queries;
using FastTests.Voron.RawData;
using SlowTests.Tests;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                var a = new UnmanagedStreamTests();
                a.BulkWriteDescendingSizeTest();
                GC.Collect(2);
                GC.WaitForPendingFinalizers();
                Console.WriteLine(i);
            }
        }
    }
}

