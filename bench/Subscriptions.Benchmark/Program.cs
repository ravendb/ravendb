using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Subscriptions.Benchmark
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            await ConcurrentBenchmark();
        }

        public static Task TestSingleConnectionSubscription(string[] args)
        {
            var paramDictionary = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            foreach (var arg in args)
            {
                var keyValue = arg.Split('=');
                paramDictionary.Add(keyValue[0], keyValue[1]);
            }

            if (paramDictionary.Count == 6)
            {
                string url = paramDictionary["url"];
                int batchSize = Int32.Parse(paramDictionary["batch"]);
                int innerParallelism = Int32.Parse(paramDictionary["ipar"]);

                var canProcceedToStressMre = paramDictionary["procceed"];
                var canProcceedToStressHandle = new EventWaitHandle(false, EventResetMode.ManualReset, canProcceedToStressMre);

                var proccessStartedMre = paramDictionary["started"];
                var reconnect = Boolean.Parse(paramDictionary["reconnect"]);

                var proccessStartedHandle = new EventWaitHandle(false, EventResetMode.ManualReset, proccessStartedMre);

                proccessStartedHandle.Set();
                canProcceedToStressHandle.WaitOne();
                var tasks = new List<Task>();

                for (var i = 0; i < innerParallelism; i++)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        int retries = 0;
                        int successfullAttempts = 0;

                        var benchmark = new SingleSubscriptionBenchmark(batchSize, url);

                        while (true)
                        {
                            if (reconnect == false)
                            {
                                benchmark?.Dispose();
                                benchmark = new SingleSubscriptionBenchmark(batchSize, url);
                            }
                            try
                            {
                                await benchmark.PerformBenchmark().ConfigureAwait(false);
                                Console.Write($"Attempt {successfullAttempts} succeeded");
                            }
                            catch (Exception e)
                            {
                                retries++;
                                Console.WriteLine($"Operation Failed, retries: {retries}, Exception: {e}");
                                benchmark?.Dispose();
                                benchmark = new SingleSubscriptionBenchmark(batchSize, url);
                                Thread.Sleep(100);
                            }
                        }
                    }));
                }
                Task.WaitAll(tasks.ToArray());
            }
            else if (args.Length < 6)
            {
                Console.ReadLine();
                string url;
                string parallelism;
                string innerParallelism;
                string batchSize;
                string reconnect;
                if (paramDictionary.TryGetValue("url", out url) == false)
                {
                    url = "http://localhost:8080";
                }
                if (paramDictionary.TryGetValue("par", out parallelism) == false)
                {
                    parallelism = "100";
                }
                if (paramDictionary.TryGetValue("ipar", out innerParallelism) == false)
                {
                    innerParallelism = "8";
                }
                if (paramDictionary.TryGetValue("batch", out batchSize) == false)
                {
                    batchSize = "1024";
                }
                if (paramDictionary.TryGetValue("reconnect", out reconnect) == false)
                {
                    reconnect = "false";
                }

                var canProcceedToStressMre = "SubsStress.CanStart";
                var canProcceedToStressHandle = new EventWaitHandle(false, EventResetMode.ManualReset, canProcceedToStressMre);
                var procs = new List<Tuple<Process, EventWaitHandle>>();

                try
                {
                    for (var i = 0; i < Int32.Parse(parallelism); i++)
                    {
                        Console.WriteLine($"Creating Proccess {i}");
                        string hasProccessBeganMreName = $"SubsStress.ProccessStarted{ i}";
                        var hasProccessBegan = new EventWaitHandle(false, EventResetMode.ManualReset, hasProccessBeganMreName);
                        string benchmarkPath;
                        if (Debugger.IsAttached)
                        {
                            benchmarkPath = "bin\\Debug\\netcoreapp1.1\\Subscriptions.Benchmark.dll";
                        }
                        else
                        {
                            benchmarkPath = "Subscriptions.Benchmark.dll";
                        }

                        var proc = Process.Start(new ProcessStartInfo()
                        {
                            Arguments = $"{benchmarkPath} url={url} batch={batchSize} ipar={innerParallelism} procceed={canProcceedToStressMre} started={hasProccessBeganMreName} reconnect={reconnect}",
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

            return Task.CompletedTask;
        }

        public static async Task ConcurrentBenchmark()
        {
            var collection = "Orders";
            var docsAmount = 10000;
            
            var concurrentBenchMark = new ConcurrentSubscriptionBenchmark(4096, "http://127.0.0.1:8080/", docsAmount, "subscriptionDB", collection);

            concurrentBenchMark.GenerateDocumentsAndRevisions(docsAmount);

            await concurrentBenchMark.PerformBenchmark(10, 1, true, true); //revision subscription with script
            
            await concurrentBenchMark.PerformBenchmark(10, 1, true, false); //docs subscription with script
            
            await concurrentBenchMark.PerformBenchmark(10, 1, false, false); //docs subscription without script

            concurrentBenchMark.DeleteDocuments();
            concurrentBenchMark.Dispose();
        }
    }
}
