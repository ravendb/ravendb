using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
            //var tests = new PageLocatorTests();
            //foreach (var cacheSize in PageLocatorTests.CacheSize)
            //{
            //    tests.TestGetReadonly(cacheSize);
            //}

            var p = new Compare();
            p.KeySize = 15;
            p.Setup();
            //p.NumericsAlt32();
            p.ScalarXorPopCount_NoCacheMisses();

            BenchmarkRunner.Run<Compare>();

            // BenchmarkSwitcher.FromAssembly(typeof(Program).GetTypeInfo().Assembly).Run(args);
        }
    }
}
