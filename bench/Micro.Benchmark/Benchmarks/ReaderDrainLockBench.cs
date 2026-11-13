using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Server;

namespace Micro.Benchmark.Benchmarks
{
    /// <summary>
    /// Measures the ReaderDrainLock reader-acquire paths.
    /// The "Token" variant mirrors the Index.cs idiom: allocate a per-call
    /// CancellationTokenSource(timeout) and pass its token to the lock.
    /// The "Try" variant uses the non-blocking entry (no CTS allocation),
    /// for where a synchronous "fail-if-busy" semantic suffices.
    /// </summary>
    [MemoryDiagnoser]
    [Config(typeof(ReaderDrainLockBench.Config))]
    public class ReaderDrainLockBench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit,
                    }
                });

                AddExporter(GetExporters().ToArray());
                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);
                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(3);

        private ReaderDrainLock _new;

        [Params(1, 4, 16)]
        public int Threads;

        [GlobalSetup]
        public void Setup()
        {
            _new = new ReaderDrainLock();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _new?.Dispose();
        }

        [Benchmark(Baseline = true)]
        public void New_EnterRead_WithToken()
        {
            if (Threads == 1)
            {
                NewOnceToken();
                return;
            }

            Parallel.For(0, Threads, _ => NewOnceToken());
        }

        private void NewOnceToken()
        {
            using CancellationTokenSource cts = new CancellationTokenSource(Timeout);
            using ReaderDrainLock.ReadHandle _ = _new.EnterRead(cts.Token);
        }

        [Benchmark]
        public void New_TryEnterRead()
        {
            if (Threads == 1)
            {
                NewOnceTry();
                return;
            }

            Parallel.For(0, Threads, _ => NewOnceTry());
        }

        private void NewOnceTry()
        {
            if (_new.TryEnterRead(out ReaderDrainLock.ReadHandle h))
                h.Dispose();
        }
    }
}
