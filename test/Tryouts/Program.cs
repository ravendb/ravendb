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
            using (var a = new FastTests.Server.Replication.ReplicationTombstoneTests())
            {
                a.Tombstones_replication_should_delete_document_at_multiple_destinations_fan();
            }
        }
    }
}

