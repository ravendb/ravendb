// See https://aka.ms/new-console-template for more information

using BenchmarkDotNet.Running;
using Integration.Benchmark;

BenchmarkSwitcher.FromAssembly(typeof(RavenDbInstance).Assembly).Run(args);
