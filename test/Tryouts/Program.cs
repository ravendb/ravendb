using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using SlowTests.Cluster;
using SlowTests.Issues;
using Sparrow;
using StressTests.Server.Replication;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.Write(".");
                using (var test = new RavenDB_13195())
                {
                    test.CanDeleteTableWithLargeValues();
                }
            }
        }
    }
}
