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
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using SlowTests.Cluster;
using Sparrow;
using StressTests.Server.Replication;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {          
        public static void Main(string[] args)
        {
            Console.WriteLine($"Press any key to start... (Process ID: {Process.GetCurrentProcess().Id})");
            Console.ReadKey();
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var test = new ClusterTransactionTests())
                {
                    test.CanPreformSeveralClusterTransactions(5).Wait();
                }
            }
        }
    }
}
