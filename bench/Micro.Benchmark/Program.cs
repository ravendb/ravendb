using System;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Running;
using Micro.Benchmark.Benchmarks.LZ4;

namespace Micro.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine($"{nameof(Sse)} support: {Sse.IsSupported}");
            Console.WriteLine($"{nameof(Sse2)} support: {Sse2.IsSupported}");
            Console.WriteLine($"{nameof(Sse3)} support: {Sse3.IsSupported}");
            Console.WriteLine($"{nameof(Sse41)} support: {Sse41.IsSupported}");

            Console.WriteLine($"{nameof(Avx)} support: {Avx.IsSupported}");
            Console.WriteLine($"{nameof(Avx2)} support: {Avx2.IsSupported}");
            Console.WriteLine($"{nameof(Avx512F)} support: {Avx512F.IsSupported}");
            Console.WriteLine($"{nameof(Avx512BW)} support: {Avx512BW.IsSupported}");
            Console.WriteLine($"{nameof(Avx512CD)} support: {Avx512CD.IsSupported}");
            Console.WriteLine($"{nameof(Avx512DQ)} support: {Avx512DQ.IsSupported}");
            Console.WriteLine($"{nameof(Avx512Vbmi)} support: {Avx512Vbmi.IsSupported}");
            Console.WriteLine($"{nameof(AvxVnni)} support: {AvxVnni.IsSupported}");

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
