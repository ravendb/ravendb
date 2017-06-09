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
           for (var i=0; i<1000; i++)
            {
                Console.WriteLine(i);
                using (var test = new FastTests.Client.Subscriptions.VersionedSubscriptions())
                {
                    test.PlainVersionedSubscriptionsCompareDocs().Wait();
                }
            }
        }
    }
}
