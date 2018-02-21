using Raven.Client.Documents;
using RachisTests.DatabaseCluster;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Utils;
using SlowTests.Server.Replication;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide;
using System.Threading;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using FastTests.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using SlowTests.Voron.Issues;
using Voron;
using Voron.Data.Tables;
using Sparrow.Binary;
using Voron.Data.Fixed;
using SlowTests.Authentication;
using FastTests.Sparrow;
using SlowTests.Server.NotificationCenter;
using FastTests;
using Xunit;
using Raven.Client.Documents.Commands.Batches;
using SlowTests.Issues;
using Raven.Client.Documents.Session;

namespace Tryouts
{
    

    class Program
    {

        public static async Task Main(string[] args)
        {
            Parallel.For(1, 10000, _ =>
             {
                 new FastTests.Blittable.PeepingTomTest().PeepingTomStreamShouldPeepCorrectlyWithRandomValues(seed: 1705655787);
             });
        }
    }

}
