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
    }
}
