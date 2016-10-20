using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
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

            BenchmarkRunner.Run<PlRandomRead>();
        }
    }
}
