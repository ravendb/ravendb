using System.Diagnostics;
using System.Linq;
using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Validators;

namespace ServerStoreTxMerger.Benchmark;
public class Program
{
    public static void Main(string[] args)
    {
        var config = new ManualConfig()
            .WithOptions(ConfigOptions.DisableOptimizationsValidator)
            .AddValidator(JitOptimizationsValidator.DontFailOnError)
            .AddLogger(ConsoleLogger.Default)
            .AddColumnProvider(DefaultColumnProviders.Instance)
            .AddJob(Job.Default
                .WithIterationCount(10)
                .WithWarmupCount(5)
                .AsDefault());
        
        var summary = BenchmarkRunner.Run(typeof(Program).Assembly, config);

        // RunTestManually().Wait();
    }

    public static async Task RunTestManually()
    {
        var count = 5;

        var time = TimeSpan.Zero;
        for (int i = 0; i < count; i++)
        {
            var test = new ServerStoreTxMergerBenchRealClusterTests();
            test.DocsGroup = ServerStoreTxMergerBenchRealClusterTests.DocsArraysKeys.First();
            test.NumOfNodes = 1;
            Console.WriteLine($"Start {i}");
            test.BeforeTest();

            var sw = Stopwatch.StartNew();
            try
            {
                await test.ClusterTest();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            time += sw.Elapsed;
            Console.WriteLine($"time: {sw.Elapsed.Hours}:{sw.Elapsed.Minutes}:{sw.Elapsed.Seconds}");

            test.AfterTest();
        }

        Console.WriteLine($"total time: {time.Hours}:{time.Minutes}:{time.Seconds}");
        time = time / count;
        Console.WriteLine($"avg time: {time.Hours}:{time.Minutes}:{time.Seconds}");
    }
}
