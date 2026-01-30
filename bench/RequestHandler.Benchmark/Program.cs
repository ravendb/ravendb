// See https://aka.ms/new-console-template for more information

using BenchmarkDotNet.Running;
using RequestHandler.Benchmark;

BenchmarkSwitcher.FromAssembly(typeof(RequestContextScopingBenchmark).Assembly).Run(args);
