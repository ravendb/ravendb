using System;
using System.Diagnostics;
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
        public static void Main(string[] args)
        {
            for (var i = 0; i < 10; i++)
            {
                Console.WriteLine(i);

                using (var test = new Versioning())
                {
                    test.WillDeleteRevisionsIfDeleted_OnlyIfPurgeOnDeleteIsTrue().Wait();
                }
            }
        }
    }
}
