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
            using (var store = new FastTests.Client.Indexing.IndexesFromClient())
            {
                store.GetStats().Wait();
            }
        }
    }

}

