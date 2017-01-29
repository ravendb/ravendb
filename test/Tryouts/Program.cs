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
using FastTests.Server.Documents;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using FastTests.Voron.FixedSize;
using FastTests.Voron.RawData;
using FastTests.Voron.Tables;
using SlowTests.Tests;
using SlowTests.Voron;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                using (var a = new FastTests.Server.Documents.Versioning.Versioning())
                {
                    a.CanGetAllRevisionsFor().Wait();
                }
                Console.WriteLine($"{i} finished");
            }

        }
    }
}
