// -----------------------------------------------------------------------
//  <copyright file="DiskIOTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using Raven.Database.DiskIO;
using Xunit;

namespace Raven.SlowTests.DiskIO
{
    public class DiskIOTest
    {

        [Fact]
        public void TestSequentialWrite()
        {
            var performanceRequest = new PerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Write,
                Path = Path.Combine(Path.GetTempPath(), "data.ravendb-io-test"),
                Sequential = true,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = new DiskPerformanceTester(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalRead);
            Assert.True(result.TotalWrite > 0);
        }

        [Fact]
        public void TestSequentialRead()
        {
            var performanceRequest = new PerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Read,
                Path = Path.Combine(Path.GetTempPath(), "data.ravendb-io-test"),
                Sequential = true,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = new DiskPerformanceTester(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalWrite);
            Assert.True(result.TotalRead > 0);
        }

        [Fact]
        public void TestRandomWrite()
        {
            var performanceRequest = new PerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Write,
                Path = Path.Combine(Path.GetTempPath(), "data.ravendb-io-test"),
                Sequential = false,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = new DiskPerformanceTester(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalRead);
            Assert.True(result.TotalWrite > 0);
        }

        [Fact]
        public void TestRandomRead()
        {
            var performanceRequest = new PerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Read,
                Path = Path.Combine(Path.GetTempPath(), "data.ravendb-io-test"),
                Sequential = false,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = new DiskPerformanceTester(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalWrite);
            Assert.True(result.TotalRead > 0);
        }

        [Fact]
        public void TestRandomReadWrite()
        {
            var performanceRequest = new PerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Mix,
                Path = Path.Combine(Path.GetTempPath(), "data.ravendb-io-test"),
                Sequential = false,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = new DiskPerformanceTester(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.True(result.TotalWrite > 0);
            Assert.True(result.TotalRead > 0);
        }
    }
}