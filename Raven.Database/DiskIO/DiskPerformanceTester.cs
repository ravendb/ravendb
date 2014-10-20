// -----------------------------------------------------------------------
//  <copyright file="DiskPerformanceTester.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using System.Threading;
using metrics.Core;

namespace Raven.Database.DiskIO
{
    internal class DiskPerformanceTester
    {
        private readonly PerformanceTestRequest testRequest;

        private readonly Action<string> onInfo;

        private readonly Action<DiskPerformanceResult> onData;

        private readonly CancellationTokenSource cts;

        private readonly List<Thread> threads;

        private readonly DiskPerformanceResult result;

        private readonly List<Random> perThreadRandom;

        private long lastTotalWrite;
        
        private long lastTotalRead;

        private Timer secondTimer;

        private long statCounter;

        public DiskPerformanceResult Result
        {
            get { return result; }
        }

        internal DiskPerformanceTester(PerformanceTestRequest testRequest, Action<string> onInfo, Action<DiskPerformanceResult> onData)
        {
            VerifyFileDoesNotExists(testRequest.Path);
            this.testRequest = testRequest;
            this.onInfo = onInfo;
            this.onData = onData;
            cts = new CancellationTokenSource();
            threads = new List<Thread>(testRequest.ThreadCount);
            result = new DiskPerformanceResult();
            perThreadRandom = Enumerable.Range(1, testRequest.ThreadCount)
                .Select(i => testRequest.RandomSeed.HasValue ? new Random(testRequest.RandomSeed.Value) : new Random()).ToList();

            if (testRequest.Sequential && testRequest.OperationType == OperationType.Mix)
            {
                onInfo("Sequential test with mixed read/write mode is not supported. Changing to random access");
                testRequest.Sequential = false;
            }
        }

        /// <summary>
        /// For security reasons we don't allow to create temporary file for performance test
        /// Without this check user can overwrite any file on disk
        /// </summary>
        /// <param name="path"></param>
        private void VerifyFileDoesNotExists(string path)
        {
            if (File.Exists(path))
            {
                throw new SecurityException("For security reason temporary file for disk performance test must not exist.");
            }
        }

        public void TestDiskIO()
        {
            try
            {
                PrepareTestFile();
                onInfo("Starting test...");
                StartWorkers();
                onInfo("Waiting for all workers to complete");
                threads.ForEach(t => t.Join());
            }
            finally
            {
                onInfo("Deleting temporary file");
                File.Delete(testRequest.Path);
            }
        }

        private void PrepareTestFile()
        {
            onInfo(string.Format("Creating test file with size = {0}", testRequest.FileSize));
            using (var fs = new FileStream(testRequest.Path, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
            {
                const int bufferSize = 4 * 1024;
                var buffer = new byte[bufferSize];
                var random = new Random();

                for (long i = 0; i < testRequest.FileSize; i += bufferSize)
                {
                    random.NextBytes(buffer);
                    fs.Write(buffer, 0, bufferSize);
                }
            }
            onInfo(string.Format("Test file created with size = {0}", testRequest.FileSize));
        }

        private void StartWorkers()
        {
            var stripeSize = testRequest.FileSize / testRequest.ThreadCount;
            for (var i = 0; i < testRequest.ThreadCount; i++)
            {
                var threadNumber = i;
                threads.Add(new Thread(() => MeasurePerformance(cts.Token, perThreadRandom[threadNumber], threadNumber * stripeSize, (threadNumber + 1) * stripeSize)));
            }
            secondTimer = new Timer(SecondTicked, null, 1000, 1000);
            threads.ForEach(t => t.Start());
        }

        private void SecondTicked(object state)
        {
            statCounter++;
            var totalRead = result.TotalRead;
            var totalWrite = result.TotalWritten;
            result.UpdateHistograms(totalRead - lastTotalRead, totalWrite - lastTotalWrite);
            lastTotalRead = totalRead;
            lastTotalWrite = totalWrite;
            onData(Result);
            if (statCounter >= testRequest.TimeToRunInSeconds)
            {
                cts.Cancel();
                secondTimer.Dispose();
            }
        }

        private void MeasurePerformance(CancellationToken token, Random random, long start, long end)
        {
            if (token.IsCancellationRequested)
            {
                return;
            }
            if (testRequest.Sequential)
            {
                switch (testRequest.OperationType)
                {
                    case OperationType.Read:
                        TestSequentialRead(token, start, end);
                        break;
                    case OperationType.Write:
                        TestSequentialWrite(token, random, start, end);
                        break;
                    default:
                        throw new NotSupportedException(string.Format("Operation type {0} is not supported for sequential", testRequest.OperationType));
                }
            }
            else
            {
                switch (testRequest.OperationType)
                {
                    case OperationType.Read:
                        TestRandomRead(token, random, start, end);
                        break;
                    case OperationType.Write:
                        TestRandomWrite(token, random, start, end);
                        break;
                    case OperationType.Mix:
                        TestRandomReadWrite(token, random, start, end);
                        break;
                }
            }
        }

        long LongRandom(long min, long max, Random rand)
        {
            var buf = new byte[8];
            rand.NextBytes(buf);
            var longRand = BitConverter.ToInt64(buf, 0);
            return (Math.Abs(longRand % (max - min)) + min);
        }

        private void TestRandomReadWrite(CancellationToken token, Random random, long start, long end)
        {
            using (var fs = new FileStream(testRequest.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                var buffer = new byte[testRequest.ChunkSize];
                while (token.IsCancellationRequested == false)
                {
                    var position = LongRandom(start, end - testRequest.ChunkSize, random);
                    fs.Seek(position, SeekOrigin.Begin);
                    if (random.Next(2) > 0)
                    {
                        fs.Read(buffer, 0, testRequest.ChunkSize);
                        result.MarkRead(testRequest.ChunkSize);
                    }
                    else
                    {
                        random.NextBytes(buffer);
                        fs.Write(buffer, 0, testRequest.ChunkSize);
                        fs.Flush(true);
                        result.MarkWrite(testRequest.ChunkSize);
                    }
                }
            }
        }

        private void TestRandomWrite(CancellationToken token, Random random, long start, long end)
        {
            using (var fs = new FileStream(testRequest.Path, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
            {
                var buffer = new byte[testRequest.ChunkSize];
                while (token.IsCancellationRequested == false)
                {
                    random.NextBytes(buffer);
                    var position = LongRandom(start, end - testRequest.ChunkSize, random);
                    fs.Seek(position, SeekOrigin.Begin);
                    fs.Write(buffer, 0, testRequest.ChunkSize);
                    fs.Flush(true);
                    result.MarkWrite(testRequest.ChunkSize);
                }
            }
        }

        private void TestRandomRead(CancellationToken token, Random random, long start, long end)
        {
            using (var fs = new FileStream(testRequest.Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var buffer = new byte[testRequest.ChunkSize];
                while (token.IsCancellationRequested == false)
                {
                    var position = LongRandom(start, end - testRequest.ChunkSize, random);
                    fs.Seek(position, SeekOrigin.Begin);
                    fs.Read(buffer, 0, testRequest.ChunkSize);
                    result.MarkRead(testRequest.ChunkSize);
                }
            }
        }

        private void TestSequentialWrite(CancellationToken token, Random random, long start, long end)
        {
            using (var fs = new FileStream(testRequest.Path, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
            {
                var buffer = new byte[testRequest.ChunkSize];
                var position = start;
                fs.Seek(position, SeekOrigin.Begin);
                while (token.IsCancellationRequested == false)
                {
                    random.NextBytes(buffer);
                    fs.Write(buffer, 0, testRequest.ChunkSize);
                    fs.Flush(true);
                    result.MarkWrite(testRequest.ChunkSize);
                    position += testRequest.ChunkSize;
                    if (position + testRequest.ChunkSize > end)
                    {
                        fs.Seek(start, SeekOrigin.Begin);
                        position = start;
                    }
                }
            }
        }

        private void TestSequentialRead(CancellationToken token, long start, long end)
        {
            using (var fs = new FileStream(testRequest.Path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var buffer = new byte[testRequest.ChunkSize];
                var position = start;
                fs.Seek(position, SeekOrigin.Begin);
                while (token.IsCancellationRequested == false)
                {
                    fs.Read(buffer, 0, testRequest.ChunkSize);
                    result.MarkRead(testRequest.ChunkSize);
                    position += testRequest.ChunkSize;
                    if (position + testRequest.ChunkSize > end)
                    {
                        fs.Seek(start, SeekOrigin.Begin);
                        position = start;
                    }
                }
            }
        }
    }

    class DiskPerformanceResult
    {
        public int ReadCount { get; private set; }
        public int WriteCount { get; private set; }
        public long TotalWritten { get; private set; }
        public long TotalRead { get; private set; }
        public PerSecondCounterMetric WriteMetric { get; private set; }
        public PerSecondCounterMetric ReadMetric { get; private  set; }
        public HistogramMetric WriteHistogram { get; private set; }
        public HistogramMetric ReadHistogram { get; private set; }

        public DiskPerformanceResult()
        {
            WriteMetric = PerSecondCounterMetric.New("write");
            ReadMetric = PerSecondCounterMetric.New("read");
            WriteHistogram = new HistogramMetric(HistogramMetric.SampleType.Uniform);
            ReadHistogram = new HistogramMetric(HistogramMetric.SampleType.Uniform);
        }

        public void UpdateHistograms(long readDelta, long writeDelta)
        {
            WriteHistogram.Update(writeDelta);
            ReadHistogram.Update(readDelta);
        }

        public void MarkRead(int count)
        {
            ReadMetric.Mark(count);
            ReadCount++;
            TotalRead += count;
        }

        public void MarkWrite(int count)
        {
            WriteMetric.Mark(count);
            WriteCount++;
            TotalWritten += count;
        }
    }
}