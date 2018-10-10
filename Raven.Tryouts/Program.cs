using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Powershell;
using Raven.Smuggler;
using Raven.Tests.Bugs;
using Raven.Tests.Common;
using Raven.Tests.FileSystem;
using Raven.Tests.Raft.Client;
using Raven.Tests.Smuggler;
using Raven.Tests.Subscriptions;
using Xunit;
using Order = Raven.Tests.Common.Dto.Faceted.Order;
using Raven.Tests.Raft;
using Raven.Tests.Faceted;
using Raven.Abstractions.Replication;
using Raven.Tests.Bundles.LiveTest;
using Raven.Tests.Core.BulkInsert;
using Raven.Tests.Notifications;
#if !DNXCORE50
using Raven.Tests.Sorting;
using Raven.SlowTests.RavenThreadPool;
using Raven.Tests.Core;
using Raven.Tests.Core.Commands;
using Raven.Tests.Issues;
using Raven.Tests.MailingList;
using Raven.Tests.FileSystem.ClientApi;
#endif

namespace Raven.Tryouts
{


    public class Program
    {
        public static void Main(string[] args)
        {
            var fixture = new TestServerFixture();
            for (var i = 0; i < 100000; i++)
            {
                try
                {
                    Console.WriteLine(i);
                    using (var test = new ChunkedBulkInsert())
                    {
                        test.SetFixture(fixture);
                        test.DocumentsInChunkConstraint();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Console.Read();
                }
            }


        }      

    
    }
}
