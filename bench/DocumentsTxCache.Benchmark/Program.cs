using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BenchmarkDotNet.Validators;

namespace DocumentsTxCache.Benchmark;

public class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "--memstats")
        {
            var mode = args.Length > 1 ? Enum.Parse<StorageMode>(args[1], ignoreCase: true) : StorageMode.Plain;
            await MemStats.Run(mode);
            return;
        }

        // the in-process toolchain avoids BenchmarkDotNet's generated-project build,
        // which passes a global OutDir that makes the multi-targeting Sparrow TFM outputs
        // overwrite each other and fails the Raven.Server compilation
        var config = new ManualConfig()
            .WithOptions(ConfigOptions.DisableOptimizationsValidator)
            .AddValidator(JitOptimizationsValidator.DontFailOnError)
            .AddLogger(ConsoleLogger.Default)
            .AddColumnProvider(DefaultColumnProviders.Instance)
            .AddJob(Job.Default
                .WithToolchain(InProcessEmitToolchain.Instance)
                .WithWarmupCount(5)
                .WithIterationCount(10)
                .AsDefault());

        BenchmarkRunner.Run(typeof(Program).Assembly, config);
    }
}
