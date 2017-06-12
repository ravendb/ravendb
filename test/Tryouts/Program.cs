using System;
using System.Diagnostics;
using System.Threading;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using FastTests.Server.Documents.Versioning;
using FastTests.Server.Replication;
using FastTests.Smuggler;
using Raven.Server.Utils;
using Raven.Client.Documents;
using RachisTests;
using Raven.Client.Util;

namespace Tryouts
{
    public class Program
    {
        public static void Hang()
        {
            ThreadPool.QueueUserWorkItem(state =>
            {
                Thread.Sleep(100);
                Hang();
            });
        }

        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (var i = 0; i < 10; i++)
            {
                Console.WriteLine(i);

                for (int j = 0; j < i+5; j++)
                {
                    Hang();
                }

                Parallel.For(0, 10, _ =>
                {
                    using (var test = new RachisTests.ElectionTests())
                    {
                        test.Follower_as_a_single_node_becomes_leader_automatically().Wait();
                    }
                });
            }
        }
    }
}
