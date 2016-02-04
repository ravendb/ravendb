// -----------------------------------------------------------------------
//  <copyright file="DiskIOTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Database.DiskIO;
using Xunit;

namespace Raven.SlowTests.DiskIO
{
    public class DiskIOTest
    {

        [Fact]
        public void TestSequentialWrite()
        {
            var performanceRequest = new GenericPerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Write,
                Path = Path.GetTempPath(),
                Sequential = true,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = AbstractDiskPerformanceTester.ForRequest(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalRead);
            Assert.True(result.TotalWrite > 0);
        }

        [Fact]
        public void TestSequentialRead()
        {
            var performanceRequest = new GenericPerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Read,
                Path = Path.GetTempPath(),
                Sequential = true,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = AbstractDiskPerformanceTester.ForRequest(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalWrite);
            Assert.True(result.TotalRead > 0);
        }

        [Fact]
        public void TestRandomWrite()
        {
            var performanceRequest = new GenericPerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Write,
                Path = Path.GetTempPath(),
                Sequential = false,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = AbstractDiskPerformanceTester.ForRequest(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalRead);
            Assert.True(result.TotalWrite > 0);
        }

        [Fact]
        public void TestRandomRead()
        {
            var performanceRequest = new GenericPerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Read,
                Path = Path.GetTempPath(),
                Sequential = false,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = AbstractDiskPerformanceTester.ForRequest(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.Equal(0, result.TotalWrite);
            Assert.True(result.TotalRead > 0);
        }

        [Fact]
        public void TestRandomReadWrite()
        {
            var performanceRequest = new GenericPerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Mix,
                Path = Path.GetTempPath(),
                Sequential = false,
                ThreadCount = 4,
                TimeToRunInSeconds = 5,
                ChunkSize = 4 * 1024
            };

            var tester = AbstractDiskPerformanceTester.ForRequest(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.True(result.TotalWrite > 0);
            Assert.True(result.TotalRead > 0);
        }

        [Fact]
        public void TestCanCancelPerformanceTest()
        {
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(5));

            var performanceRequest = new GenericPerformanceTestRequest
            {
                FileSize = (long)128 * 1024,
                OperationType = OperationType.Mix,
                Path = Path.GetTempPath(),
                Sequential = false,
                ThreadCount = 4,
                TimeToRunInSeconds = 30,
                ChunkSize = 4 * 1024
            };

            var tester = AbstractDiskPerformanceTester.ForRequest(performanceRequest, s => { }, cts.Token);
            Assert.Throws<OperationCanceledException>(() => tester.TestDiskIO());
        }

        [Fact]
        public void TestBatchPerformanceTest()
        {
            var performanceRequest = new BatchPerformanceTestRequest
            {
                FileSize = (long) 1024*1024*1024,
                NumberOfDocuments = 1000000,
                NumberOfDocumentsInBatch = 200,
                Path = Path.GetTempPath(),
                SizeOfDocuments = 756,
                WaitBetweenBatches = 5
            };

            var tester = AbstractDiskPerformanceTester.ForRequest(performanceRequest, s => { });
            tester.TestDiskIO();
            var result = tester.Result;
            Assert.True(result.TotalWrite > 0);
            Assert.Equal(0, result.TotalRead);
        }
    }
}
