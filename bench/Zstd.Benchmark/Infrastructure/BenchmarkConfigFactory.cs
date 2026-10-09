using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;

namespace Zstd.Benchmark.Infrastructure
{
    internal sealed class LibraryUnderTest
    {
        public string Id;
        public string Path;
        public string Version;
    }

    internal sealed class RunOptions
    {
        public List<LibraryUnderTest> Libraries = new();
        public string Affinity = "pcores";
        public int Launches = 2;
        public int Iterations = 15;
        public int Warmups = 3;
        public string ArtifactsPath;
    }

    internal static class BenchmarkConfigFactory
    {
        public static IConfig Create(RunOptions options)
        {
            IntPtr? affinity = ResolveAffinity(options.Affinity, out string affinityDescription);
            Console.WriteLine($"// [zstd-bench] affinity: {affinityDescription}");

            ManualConfig config = ManualConfig.Create(DefaultConfig.Instance)
                .AddDiagnoser(MemoryDiagnoser.Default)
                .AddColumn(new ThroughputColumn())
                .AddExporter(JsonExporter.Full)
                .AddValidator(JitOptimizationsValidator.FailOnError)
                .HideColumns("EnvironmentVariables", "Affinity", "IterationCount", "LaunchCount", "WarmupCount")
                .WithSummaryStyle(BenchmarkDotNet.Reports.SummaryStyle.Default.WithMaxParameterColumnWidth(40));

            if (string.IsNullOrEmpty(options.ArtifactsPath) == false)
                config = config.WithArtifactsPath(options.ArtifactsPath);

            bool first = true;
            foreach (LibraryUnderTest library in options.Libraries)
            {
                Job job = Job.Default
                    .WithId(library.Id)
                    .WithLaunchCount(options.Launches)
                    .WithWarmupCount(options.Warmups)
                    .WithIterationCount(options.Iterations)
                    .WithEnvironmentVariables(
                        new EnvironmentVariable(NativeLibrarySelector.LibraryPathVariable, library.Path),
                        new EnvironmentVariable(NativeLibrarySelector.ExpectedVersionVariable, library.Version));

                if (affinity.HasValue)
                    job = job.WithAffinity(affinity.Value);

                if (first && options.Libraries.Count > 1)
                    job = job.AsBaseline();

                config.AddJob(job);
                first = false;
            }

            return config;
        }

        private static IntPtr? ResolveAffinity(string value, out string description)
        {
            switch (value?.ToLowerInvariant())
            {
                case null:
                case "":
                case "none":
                    description = "none";
                    return null;
                case "pcores":
                    return CpuTopology.GetPerformanceCoresMask(out description);
                default:
                    string hex = value.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? value[2..] : value;
                    long mask = Convert.ToInt64(hex, 16);
                    description = $"mask 0x{mask:X}";
                    return (IntPtr)mask;
            }
        }
    }
}
