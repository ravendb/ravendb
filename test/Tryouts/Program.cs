using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Alerts;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using FastTests.Server.Documents.SqlReplication;
using SlowTests.Core.Commands;
using Sparrow.Json;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            for (int i = 0; i < 10; i++)
            {
                using (var a = new SlowTests.Issues.RavenDB_5763())
                {
                    a.Should_not_throw_timeout_and_out_of_memory();
                }
            }
        }
    }

}

