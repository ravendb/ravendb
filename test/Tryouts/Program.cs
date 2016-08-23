using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;
using SlowTests.Tests.Linq;

namespace Tryouts
{

    public class Program
    {
        static void Main(string[] args)
        {

            using (var f = new FastTests.Server.Documents.Replication.ReplicationConflictsTests())
            {
                f.Conflict_should_work_on_master_slave_slave().Wait();
            }

        }
    }
}

