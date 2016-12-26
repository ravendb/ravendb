using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Alerts;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using FastTests.Server.Documents.SqlReplication;
using Raven.Client.Document;
using SlowTests.Core.Commands;
using Sparrow.Json;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var a = new FastTests.Client.Subscriptions.Subscriptions())
                {
                    a.SubscriptionSimpleTakeOverStrategy().Wait();
                }
            }
        }
    }

}

