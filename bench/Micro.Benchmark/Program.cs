using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Intrinsics.X86;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using Micro.Benchmark.Benchmarks.Hardware;
using Micro.Benchmark.Benchmarks.PageLocator;
using Micro.Benchmark.Tests;

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


            //var tests = new PageLocatorTests();
            //foreach (var cacheSize in PageLocatorTests.CacheSize)
            //{
            //    tests.TestGetReadonly(cacheSize);
            //}

            var p = new DiffNonZeroes();
            //p.KeySize = 15;
            p.Setup();
            //p.NumericsAlt32();

            for (int i = 0; i < 5000; i++)
                p.Original_Sequential();

            //for (int i = 0; i < 4000; i++)
            //    p.Original_Sequential();

            //for (int i = 0; i < 4000; i++)
            //    p.Numerics32_Sequential();

            //BenchmarkRunner.Run<DiffNonZeroes>();

            // BenchmarkSwitcher.FromAssembly(typeof(Program).GetTypeInfo().Assembly).Run(args);
        }
    }
}
