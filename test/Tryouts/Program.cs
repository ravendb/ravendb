using System;
using System.Diagnostics;
using System.Reflection;
using FastTests.Client.Attachments;
using RachisTests.DatabaseCluster;
using Sparrow.Logging;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using Raven.Server.Utils;
using Raven.Client.Documents;
using RachisTests;
using Raven.Client.Util;
using System.Threading;

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
            var setMinThreads = (Func<int, int, bool>)typeof(ThreadPool).GetTypeInfo().GetMethod("SetMinThreads")
                .CreateDelegate(typeof(Func<int, int, bool>));

            setMinThreads(250, 250);


            LoggingSource.Instance.SetupLogMode(LogMode.Information, "Logs");
            for (var i = 0; i < 100; i++)
            {
                Console.WriteLine(i);

                Parallel.For(0, 10, _ =>
                {
                    using (var a = new FastTests.Tasks.RavenDB_6886())
                    {
                        a.Cluster_identity_for_multiple_documents_on_different_nodes_should_work().Wait();
                    }
                });
            }
        }
    }
}
