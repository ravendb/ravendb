using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Micro.Benchmark.PageLocatorImpl;

namespace Micro.Benchmark.Benchmarks.PageLocator
{
    [Config(typeof(Config))]
    public class PlRandomRead
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                Add(new Job
                {
                    Environment =
                    {
                        Runtime = Runtime.Core,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit
                    },
                    // TODO: Next line is just for testing. Fine tune parameters.
                    //Run =
                    //{
                    //    RunStrategy = RunStrategy.Monitoring,
                    //    LaunchCount = 1,
                    //    WarmupCount = 2,
                    //    TargetCount = 40
                    //}
                });

                // Exporters for data
                Add(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                Add(RPlotExporter.Default);

                Add(StatisticColumn.AllStatistics);

                Add(BaselineValidator.FailOnError);
                Add(JitOptimizationsValidator.FailOnError);

                Add(EnvironmentAnalyser.Default);
            }
        }

        private const int NumberOfOperations = 10000;

        [Params(8, 16, 32, 64, 128, 256)]
        public int CacheSize { get; set; }

        [Params(5)]
        public int RandomSeed { get; set; }

        private List<long> _pageNumbers;

        private PageLocatorV1 _cacheV1;
        private PageLocatorV2 _cacheV2;
        private PageLocatorV3 _cacheV3;
        private PageLocatorV4 _cacheV4;
        private PageLocatorV5 _cacheV5;
        private PageLocatorV6 _cacheV6;
        private PageLocatorV7 _cacheV7;

        [GlobalSetup]
        public void Setup()
        {
            _cacheV1 = new PageLocatorV1(null, CacheSize);
            _cacheV2 = new PageLocatorV2(null, CacheSize);
            _cacheV3 = new PageLocatorV3(null, CacheSize);
            _cacheV4 = new PageLocatorV4(null, CacheSize);
            _cacheV5 = new PageLocatorV5(null, CacheSize);
            _cacheV6 = new PageLocatorV6(null, CacheSize);
            _cacheV7 = new PageLocatorV7(null, CacheSize);

            var generator = new Random(RandomSeed);

            _pageNumbers = new List<long>();
            for (int i = 0; i < NumberOfOperations; i++)
            {
                long valueBuffer = generator.Next();
                valueBuffer += (long)generator.Next() << 32;
                valueBuffer += (long)generator.Next() << 64;
                valueBuffer += (long)generator.Next() << 96;

                _pageNumbers.Add(valueBuffer);
            }
        }

        [Benchmark(OperationsPerInvoke = NumberOfOperations)]
        public void BasicV1()
        {
            foreach (var pageNumber in _pageNumbers)
            {
                _cacheV1.GetReadOnlyPage(pageNumber);
            }
        }

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void BasicV2()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV2.GetReadOnlyPage(pageNumber);
        //    }
        //}

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void BasicV3()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV3.GetReadOnlyPage(pageNumber);
        //    }
        //}

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void BasicV4()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV4.GetReadOnlyPage(pageNumber);
        //    }
        //}

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void BasicV5()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV5.GetReadOnlyPage(pageNumber);
        //    }
        //}

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void BasicV6()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV6.GetReadOnlyPage(pageNumber);
        //    }
        //}

        [Benchmark(OperationsPerInvoke = NumberOfOperations)]
        public void BasicV7()
        {
            foreach (var pageNumber in _pageNumbers)
            {
                _cacheV7.GetReadOnlyPage(pageNumber);
            }
        }

        //[Benchmark(OperationsPerInvoke = NumberOfOperations)]
        //public void BasicV8()
        //{
        //    foreach (var pageNumber in _pageNumbers)
        //    {
        //        _cacheV8.GetReadOnlyPage(pageNumber);
        //    }
        //}
    }
}
