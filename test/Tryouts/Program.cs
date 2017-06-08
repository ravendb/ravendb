using System;
using System.Diagnostics;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using Raven.Server.Utils;
using Raven.Client.Documents;
using RachisTests;
using Raven.Client.Util;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Name;
        }

        public static void Main(string[] args)
        {
           for (var i=0; i<10; i++)
            {
                Console.WriteLine(i);

                Parallel.For(0, 10, _ =>
                {
                    using (var test = new FastTests.Server.Replication.DisableDatabasePropagationInRaftCluster())
                    {
                        test.DisableDatabaseToggleOperation_should_propagate_through_raft_cluster().Wait();
                    }
                });
            }
        }
    }
}
