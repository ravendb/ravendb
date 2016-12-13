using System;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Alerts;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using FastTests.Server.Documents.SqlReplication;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            Parallel.For(0, 1000, i =>
                {
                    Console.WriteLine(i);
                    using (var store = new FastTests.Server.Documents.Replication.ReplicationTombstoneTests())
                    {
                        store.Two_tombstones_should_replicate_in_master_master().Wait();
                    }
                }
            );
        }
    }

}

