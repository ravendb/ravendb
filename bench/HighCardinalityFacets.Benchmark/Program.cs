using System;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BenchmarkDotNet.Validators;

namespace HighCardinalityFacets.Benchmark;

public class Program
{
    public static void Main(string[] args)
    {
        // the in-process toolchain avoids BenchmarkDotNet's generated-project build,
        // which passes a global OutDir that makes the multi-targeting Sparrow TFM outputs
        // overwrite each other and fails the Raven.Server compilation
        var config = new ManualConfig()
            .WithOptions(ConfigOptions.DisableOptimizationsValidator)
            .AddValidator(JitOptimizationsValidator.DontFailOnError)
            .AddLogger(ConsoleLogger.Default)
            .AddColumnProvider(DefaultColumnProviders.Instance)
            .AddJob(Job.Default
                // the default in-process timeout is five minutes and covers GlobalSetup, which seeds and indexes the data set
                .WithToolchain(new InProcessEmitToolchain(TimeSpan.FromHours(2), logOutput: true))
                .WithWarmupCount(3)
                .WithIterationCount(10)
                .AsDefault());

        try
        {
            BenchmarkRunner.Run(typeof(Program).Assembly, config, args);
        }
        finally
        {
            // the seeded database is shared by every benchmark in the run, so it is torn down once, here
            Harness.DisposeShared();
        }
    }
}
