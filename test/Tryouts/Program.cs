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
            using (var a = new FastTests.Voron.Compaction.StorageCompactionTests())
            {
                a.ShouldOccupyLessSpace();
            }
        }
    }
}

