using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using static System.Int32;


namespace SubscriptionsBenchmark
{
    

    public class Program
    {
        //public static void Main(string[] args)
        //{
        //    Console.WriteLine();
        //    Console.ReadLine();
        //    if (args.Length == 0 || args[0] == "?" || args[0] == "-help" || args[0] == "--help")
        //    {
        //        Console.WriteLine("Usage:");
        //        Console.WriteLine("(M) - Mandatory; (O) - Optional");
        //        Console.WriteLine("Subscriptions.Benchmark (M)[URL] (M)[Database Name] (M)[Collection Name] v[Parallelism Amount] (O)[Max Items]");
        //    }
        //    var url = args[0];
        //    var defaultDatabase = args[1];
        //    var collectionName = args[2];

        //    var parallelism = Int32.Parse(args[3]);
        //    int maxItems = 0;
        //    if (args.Length == 5)
        //        maxItems = Int32.Parse(args[4]);
        //    var benchmark = new ParallelSubscriptionsBenchmark();
        //    AsyncHelpers.RunSync(()=>ParallelSubscriptionsBenchmark(url, defaultDatabase, maxItems, collectionName,parallelism));
        //}
        
        public static void Main(string[] args)
        {
            //  Console.ReadLine();
            //new SingleSubscriptionBenchmark(new string[] {}).PerformBenchmark();
            //new ParallelSubscriptionsBenchmark().Run().Wait();

            //Parallel.For(0, 30, x => new SingleSubscriptionBenchmark(new string[] {}).PerformBenchmark());
            if (args.Length != 0)
                new SingleSubscriptionBenchmark(new string[] { }).PerformBenchmark();
            else
            {
                var procs = new List<Process>();
                for (var i = 0; i < 30; i++)
                {
                    Console.WriteLine($"Creating Proccess {i}");
                    var proc = Process.Start(new ProcessStartInfo()
                    {
                        Arguments = "bin\\Release\\netcoreapp1.0\\Subscriptions.Benchmark.dll trala",
                        FileName = "dotnet"
                    });

                    procs.Add(proc);
                    Console.WriteLine($"Created Proccess {i}");
                }

                foreach (var process in procs)
                {
                    if (process.HasExited == false)
                        process.WaitForExit();
                    Console.WriteLine($"Proccess Finished");
                }
            }

        }

        
    }
}
