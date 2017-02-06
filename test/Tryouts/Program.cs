using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using System.Text;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Server.Basic;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Expiration;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using FastTests.Voron.FixedSize;
using FastTests.Voron.RawData;
using FastTests.Voron.Tables;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Document;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using SlowTests.Server.Documents.SqlReplication;
using SlowTests.Tests;
using SlowTests.Voron;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Data.Tables;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //            for (int i = 0; i < 1000; i++)
            //            {
            //                Console.WriteLine(i);
            //                using (var a = new ReplicationIndexesAndTransformers())
            //                {
            //                    a.Can_replicate_multiple_indexes();
            //                }
            //            }



            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var a = new CanReplicate())
                {
                    a.ReplicateMultipleBatches().Wait();
                }
            }
        }
    }
}