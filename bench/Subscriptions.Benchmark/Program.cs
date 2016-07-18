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
        public static void Main(string[] args)
        {
            var paramDictionary = new Dictionary<string, string>();
            foreach (var arg in args)
            {
                var keyValue = arg.Split('=');
                paramDictionary.Add(keyValue[0], keyValue[1]);
            }

            if (paramDictionary.Count== 5)
            {
                
                string url = paramDictionary["url"];
                int batchSize = Int32.Parse(paramDictionary["batch"]);
                int innerParallelism = Int32.Parse(paramDictionary["ipar"]);

                var canProcceedToStressMre = paramDictionary["procceed"];
                var canProcceedToStressHandle = new EventWaitHandle(false,EventResetMode.ManualReset,canProcceedToStressMre);
                
                var proccessStartedMre = paramDictionary["started"];
                var proccessStartedHandle = new EventWaitHandle(false,EventResetMode.ManualReset,proccessStartedMre);

                proccessStartedHandle.Set();
                canProcceedToStressHandle.WaitOne();
                Parallel.For(0, innerParallelism,new ParallelOptions()
                {
                    MaxDegreeOfParallelism = innerParallelism
                } , x =>
                {
                    int retries = 0;
                    int successfullAttempts = 0;
                    while (true)
                    {
                        try
                        {
                            new SingleSubscriptionBenchmark(url, batchSize).PerformBenchmark();
                            Console.Write($"Attempt {successfullAttempts} succeeded");
                        }
                        catch (Exception e)
                        {
                            retries++;
                            Console.WriteLine($"Operation Failed, retries: {retries}, Exception: {e}");
                            Thread.Sleep(100);
                        }
                    }
                    

                });

            }
            else if (args.Length <5)
            {
                string url;
                string parallelism;
                string innerParallelism;
                string batchSize;
                if (paramDictionary.TryGetValue("url", out url) == false)
                {
                    url = "http://localhost:8080";
                }
                if (paramDictionary.TryGetValue("par", out parallelism) == false)
                {
                    parallelism= "100";
                }
                if (paramDictionary.TryGetValue("ipar", out innerParallelism) == false)
                {
                    innerParallelism = "8";
                }
                if (paramDictionary.TryGetValue("batch", out batchSize) == false)
                {
                    batchSize = "1024";
                }

                var canProcceedToStressMre = "SubsStress.CanStart";
                var canProcceedToStressHandle = new EventWaitHandle(false,EventResetMode.ManualReset,canProcceedToStressMre);
                var procs = new List<Tuple<Process,EventWaitHandle>>();

                try
                {
                    for (var i = 0; i < Int32.Parse(parallelism); i++)
                    {
                        Console.WriteLine($"Creating Proccess {i}");
                        string hasProccessBeganMreName= $"SubsStress.ProccessStarted{ i}";
                        var hasProccessBegan = new EventWaitHandle(false,EventResetMode.ManualReset,hasProccessBeganMreName);
                        var proc = Process.Start(new ProcessStartInfo()
                        {
                            Arguments = $"Subscriptions.Benchmark.dll url={url} batch={batchSize} ipar={innerParallelism} procceed={canProcceedToStressMre} started={hasProccessBeganMreName}",
                            FileName = "dotnet"
                        });

                        procs.Add(Tuple.Create(proc, hasProccessBegan));
                        Console.WriteLine($"Created Proccess {i}");
                    }

                    foreach (var process in procs)
                    {
                        process.Item2.WaitOne();
                    }

                    canProcceedToStressHandle.Set();

                    canProcceedToStressHandle.Dispose();
                }
                finally
                {
                    foreach (var process in procs)
                    {
                        if (process.Item1.HasExited == false)
                            process.Item1.WaitForExit();
                        process.Item2.Dispose();
                        Console.WriteLine($"Proccess Finished");
                    }
                    canProcceedToStressHandle.Dispose();
                }
            }

        }

        
    }
}
