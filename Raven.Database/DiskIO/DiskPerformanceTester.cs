// -----------------------------------------------------------------------
//  <copyright file="DiskPerformanceTester.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Raven.Abstractions.Data;
using Voron.Platform.Win32;
using metrics.Core;
using Raven.Database.Extensions;

namespace Raven.Database.DiskIO
{
    public class DiskPerformanceTester
    {
        public const string PerformanceResultDocumentKey = "Raven/Disk/Performance";

        public const string TemporaryFileName = "data.ravendb-io-test";

        private readonly PerformanceTestRequest testRequest;

        private readonly Action<string> onInfo;

        private readonly CancellationTokenSource testTimerCts;

        private readonly CancellationTokenSource linkedCts;

        private readonly CancellationToken taskKillToken;

        private readonly List<Thread> threads;

        private readonly DiskPerformanceStorage dataStorage;

        private readonly List<Random> perThreadRandom;

        private Timer secondTimer;

        private long statCounter;

        public DiskPerformanceResult Result
        {
            get
            {
                return new DiskPerformanceResult(dataStorage);
            }
        }

        public DiskPerformanceTester(PerformanceTestRequest testRequest, Action<string> onInfo, CancellationToken taskKillToken = default(CancellationToken))
        {
            this.testRequest = testRequest;
            this.onInfo = onInfo;
            this.taskKillToken = taskKillToken;
            testTimerCts = new CancellationTokenSource();
            linkedCts = CancellationTokenSource.CreateLinkedTokenSource(taskKillToken, testTimerCts.Token);
            threads = new List<Thread>(testRequest.ThreadCount);
            dataStorage = new DiskPerformanceStorage();
            perThreadRandom = Enumerable.Range(1, testRequest.ThreadCount)
                .Select(i => testRequest.RandomSeed.HasValue ? new Random(testRequest.RandomSeed.Value) : new Random()).ToList();

            if (testRequest.Sequential && testRequest.OperationType == OperationType.Mix)
            {
                onInfo("Sequential test with mixed read/write mode is not supported. Changing to random access");
                testRequest.Sequential = false;
            }
        }

        public void TestDiskIO()
        {
            PrepareTestFile();
            onInfo("Starting test...");
            StartWorkers();
            onInfo("Waiting for all workers to complete");
            threads.ForEach(t => t.Join());
            taskKillToken.ThrowIfCancellationRequested();
        }

        private void PrepareTestFile()
        {
            var sw = new Stopwatch();
            sw.Start();
           
            if (File.Exists(testRequest.Path))
            {
                var fInfo = new FileInfo(testRequest.Path);
                if (fInfo.Length < testRequest.FileSize)
                {
                    onInfo(string.Format("Expanding test file to {0}", testRequest.FileSize));
                    using (var fs = new FileStream(testRequest.Path, FileMode.Append, FileAccess.Write))
                    {
                        const int bufferSize = 4 * 1024;
                        var buffer = new byte[bufferSize];
                        var random = new Random();

                        for (long i = 0; i < testRequest.FileSize - fInfo.Length; i += bufferSize)
                        {
                            random.NextBytes(buffer);
                            fs.Write(buffer, 0, bufferSize);
                        }
                    }
                    var elapsed = sw.Elapsed;
                    onInfo(string.Format("Test file expanded to size: {0} in {1}", testRequest.FileSize, elapsed));
                }
                else
                {
                    onInfo("Reusing existing test file.");
                }
            }
            else
            {
                onInfo(string.Format("Creating test file with size = {0}", testRequest.FileSize));
                using (var fs = new FileStream(testRequest.Path, FileMode.OpenOrCreate, FileAccess.Write))
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
                var elapsed = sw.Elapsed;
                onInfo(string.Format("Test file created with size: {0} in {1}", testRequest.FileSize, elapsed));
            }
        }

        private void StartWorkers()
        {
            var stripeSize = testRequest.FileSize / testRequest.ThreadCount;
            for (var i = 0; i < testRequest.ThreadCount; i++)
            {
                var threadNumber = i;
                threads.Add(new Thread(() => MeasurePerformance(linkedCts.Token, perThreadRandom[threadNumber], threadNumber * stripeSize, (threadNumber + 1) * stripeSize)));
            }
            secondTimer = new Timer(SecondTicked, null, 1000, 1000);
            threads.ForEach(t => t.Start());
        }

        private void SecondTicked(object state)
        {
            statCounter++;

            dataStorage.Update();

            if (statCounter >= testRequest.TimeToRunInSeconds)
            {
                testTimerCts.Cancel();
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
                        TestSequentialRead(token);
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
                        TestRandomRead(token, random);
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

        long LongRandom(long min, long max, int mutlipleOf, Random rand)
        {
            var buf = new byte[8];
            rand.NextBytes(buf);
            var longRand = BitConverter.ToInt64(buf, 0);
            var value = (Math.Abs(longRand % (max - min)) + min);
            return value/mutlipleOf*mutlipleOf;
        }
        
        /// <summary>
        /// Each thread write to individual section
        /// Each thread reads from any section
        /// </summary>
        /// <param name="token"></param>
        /// <param name="random"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        private void TestRandomReadWrite(CancellationToken token, Random random, long start, long end)
        {
            using (var handle = Win32NativeFileMethods.CreateFile(testRequest.Path,
                                                                  Win32NativeFileAccess.GenericWrite | Win32NativeFileAccess.GenericRead, 
                                                                  Win32NativeFileShare.Read | Win32NativeFileShare.Write, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  Win32NativeFileAttributes.Write_Through | 
                                                                    (testRequest.Buffered ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering), 
                                                                  IntPtr.Zero))
            {
                ValidateHandle(handle);
                using (var fs = new FileStream(handle, FileAccess.ReadWrite))
                {
                    var buffer = new byte[testRequest.ChunkSize];
                    var sw = new Stopwatch();
                    while (token.IsCancellationRequested == false)
                    {
                        if (random.Next(2) > 0)
                        {
                            var position = LongRandom(0, testRequest.FileSize, 4096, random);
                            sw.Restart();
                            fs.Seek(position, SeekOrigin.Begin);
                            fs.Read(buffer, 0, testRequest.ChunkSize);
                            dataStorage.MarkRead(testRequest.ChunkSize, sw.ElapsedMilliseconds);
                        }
                        else
                        {
                            var position = LongRandom(start, end - testRequest.ChunkSize, 4096, random);
                            random.NextBytes(buffer);
                            sw.Restart();
                            fs.Seek(position, SeekOrigin.Begin);
                            fs.Write(buffer, 0, testRequest.ChunkSize);
                            dataStorage.MarkWrite(testRequest.ChunkSize, sw.ElapsedMilliseconds);
                        }
                    }
                }
            }
        }
        
        /// <summary>
        ///  Each thread write to individual section
        /// </summary>
        /// <param name="token"></param>
        /// <param name="random"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        private void TestRandomWrite(CancellationToken token, Random random, long start, long end)
        {
            using (var handle = Win32NativeFileMethods.CreateFile(testRequest.Path,
                                                                  Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.Write, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  Win32NativeFileAttributes.Write_Through |
                                                                    (testRequest.Buffered ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering),
                                                                  IntPtr.Zero))
            {
                ValidateHandle(handle);

                using (var fs = new FileStream(handle, FileAccess.Write))
                {
                    var buffer = new byte[testRequest.ChunkSize];
                    var sw = new Stopwatch();
                    while (token.IsCancellationRequested == false)
                    {
                        random.NextBytes(buffer);
                        var position = LongRandom(start, end - testRequest.ChunkSize, 4096, random);
                        sw.Restart();
                        fs.Seek(position, SeekOrigin.Begin);
                        fs.Write(buffer, 0, testRequest.ChunkSize);
                        dataStorage.MarkWrite(testRequest.ChunkSize, sw.ElapsedMilliseconds);
                    }
                }
            }
        }

        /// <summary>
        /// Each thread reads from any section
        /// </summary>
        /// <param name="token"></param>
        /// <param name="random"></param>
        private void TestRandomRead(CancellationToken token, Random random)
        {
            using (var handle = Win32NativeFileMethods.CreateFile(testRequest.Path,
                                                     Win32NativeFileAccess.GenericRead, Win32NativeFileShare.Read, IntPtr.Zero,
                                                     Win32NativeFileCreationDisposition.OpenExisting,
                                                     testRequest.Buffered ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering,
                                                     IntPtr.Zero))
            {
                ValidateHandle(handle);
                using (var fs = new FileStream(handle, FileAccess.Read))
                {
                    var buffer = new byte[testRequest.ChunkSize];
                    var sw = new Stopwatch();
                    while (token.IsCancellationRequested == false)
                    {
                        var position = LongRandom(0, testRequest.FileSize, 4096, random);
                        sw.Restart();
                        // for with with no_buffering seek must be multiple of 4096.
                        fs.Seek(position, SeekOrigin.Begin);
                        fs.Read(buffer, 0, testRequest.ChunkSize);
                        dataStorage.MarkRead(testRequest.ChunkSize, sw.ElapsedMilliseconds);
                    }
                }
            }
        }

        /// <summary>
        ///  Each thread write to individual section
        /// </summary>
        /// <param name="token"></param>
        /// <param name="random"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        private void TestSequentialWrite(CancellationToken token, Random random, long start, long end)
        {
            using (var handle = Win32NativeFileMethods.CreateFile(testRequest.Path,
                                                                  Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.Write, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  Win32NativeFileAttributes.Write_Through | 
                                                                    (testRequest.Buffered ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering), 
                                                                  IntPtr.Zero))
            {
               ValidateHandle(handle);
                using (var fs = new FileStream(handle, FileAccess.ReadWrite))
                {
                    var buffer = new byte[testRequest.ChunkSize];
                    var position = start;
                    fs.Seek(position, SeekOrigin.Begin);
                    var sw = new Stopwatch();
                    while (token.IsCancellationRequested == false)
                    {
                        random.NextBytes(buffer);
                        sw.Restart();
                        fs.Write(buffer, 0, testRequest.ChunkSize);
                        dataStorage.MarkWrite(testRequest.ChunkSize, sw.ElapsedMilliseconds);
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

        /// <summary>
        /// Each thread reads sequentially from beginning of file
        /// </summary>
        /// <param name="token"></param>
        private void TestSequentialRead(CancellationToken token)
        {
            using (var handle = Win32NativeFileMethods.CreateFile(testRequest.Path,
                                                                  Win32NativeFileAccess.GenericRead, Win32NativeFileShare.Read, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  testRequest.Buffered ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering, 
                                                                  IntPtr.Zero))
            {
                ValidateHandle(handle);

                using (var fs = new FileStream(handle, FileAccess.Read))
                {
                    var buffer = new byte[testRequest.ChunkSize];
                    var position = 0;
                    fs.Seek(position, SeekOrigin.Begin);
                    var sw = new Stopwatch();
                    while (token.IsCancellationRequested == false)
                    {
                        sw.Restart();
                        fs.Read(buffer, 0, testRequest.ChunkSize);
                        dataStorage.MarkRead(testRequest.ChunkSize, sw.ElapsedMilliseconds);
                        position += testRequest.ChunkSize;
                        if (position + testRequest.ChunkSize > testRequest.FileSize)
                        {
                            fs.Seek(0, SeekOrigin.Begin);
                            position = 0;
                        }
                    }
                }
            }
        }

        private static void ValidateHandle(SafeFileHandle handle)
        {
            if (handle.IsInvalid)
            {
                int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open test file",
                                      new Win32Exception(lastWin32ErrorCode));
            }
        }

        public void DescribeTestParameters()
        {
            var action = "read/write";
            if (testRequest.OperationType == OperationType.Write)
            {
                action = "write";
            } else if (testRequest.OperationType == OperationType.Read)
            {
                action = "read";
            }

            Console.WriteLine("{0} threads {1} {2} {3} for {4} seconds from file {5} (size = {6} MB) in {7} kb chunks.", 
                testRequest.ThreadCount, action, testRequest.Buffered ? "buffered" : "unbuffered", 
                testRequest.Sequential ? "sequential" : "random", testRequest.TimeToRunInSeconds,
                testRequest.Path, testRequest.FileSize / 1024 / 1024, testRequest.ChunkSize / 1024);
        }
    }

    public class DiskPerformanceStorage
    {
        public List<long> ReadPerSecondHistory { get; private set; }
        public List<long> WritePerSecondHistory { get; private set; }
        public List<double> AverageReadLatencyPerSecondHistory { get; private set; }
        public List<double> AverageWriteLatencyPerSecondHistory { get; private set; }
        public HistogramMetric WriteLatencyHistogram { get; private set; }
        public HistogramMetric ReadLatencyHistogram { get; private set; }

        private long totalReadLatencyInCurrentSecond;
        private long readEventsInCurrentSecond;

        private long totalWriteLatencyInCurrentSecond;
        private long writeEventsInCurrentSecond;

        private long totalRead;
        private long totalWrite;
        private long lastTotalWrite;
        private long lastTotalRead;

        public long TotalRead
        {
            get { return totalRead; }
        }
        public long TotalWrite
        {
            get { return totalWrite; }
        }

        public DiskPerformanceStorage()
        {
            WriteLatencyHistogram = new HistogramMetric(HistogramMetric.SampleType.Uniform);
            ReadLatencyHistogram = new HistogramMetric(HistogramMetric.SampleType.Uniform);
            ReadPerSecondHistory = new List<long>();
            WritePerSecondHistory = new List<long>();
            AverageReadLatencyPerSecondHistory = new List<double>();
            AverageWriteLatencyPerSecondHistory = new List<double>();
        }

        public void Update()
        {
            lock (this)
            {
                ReadPerSecondHistory.Add(totalRead - lastTotalRead);
                WritePerSecondHistory.Add(totalWrite - lastTotalWrite);
                lastTotalRead = totalRead;
                lastTotalWrite = totalWrite;

                AverageReadLatencyPerSecondHistory.Add(readEventsInCurrentSecond > 0 ? totalReadLatencyInCurrentSecond * 1.0 / readEventsInCurrentSecond : 0);
                AverageWriteLatencyPerSecondHistory.Add(writeEventsInCurrentSecond > 0 ? totalWriteLatencyInCurrentSecond * 1.0 / writeEventsInCurrentSecond : 0);
            }
          
        }

        public void MarkRead(long size, long latency)
        {
            lock (this)
            {
                ReadLatencyHistogram.Update(latency);
                totalRead += size;
                readEventsInCurrentSecond++;
                totalReadLatencyInCurrentSecond += latency;
            }
        }

        public void MarkWrite(long size, long latency)
        {
            lock (this)
            {
                WriteLatencyHistogram.Update(latency);
                totalWrite += size;
                writeEventsInCurrentSecond++;
                totalWriteLatencyInCurrentSecond += latency;
            }
        }
    }

    public class DiskPerformanceResult
    {
        public List<long> ReadPerSecondHistory { get; private set; }
        public List<long> WritePerSecondHistory { get; private set; }
        public List<double> AverageReadLatencyPerSecondHistory { get; private set; }
        public List<double> AverageWriteLatencyPerSecondHistory { get; private set; }
        public HistogramData ReadLatency { get; private set; }
        public HistogramData WriteLatency { get; private set; }

        public long TotalRead { get; private set; }
        public long TotalWrite { get; private set; }

        public DiskPerformanceResult(DiskPerformanceStorage storage)
        {
            ReadPerSecondHistory = new List<long>(storage.ReadPerSecondHistory);
            WritePerSecondHistory = new List<long>(storage.WritePerSecondHistory);
            AverageReadLatencyPerSecondHistory = new List<double>(storage.AverageReadLatencyPerSecondHistory);
            AverageWriteLatencyPerSecondHistory = new List<double>(storage.AverageWriteLatencyPerSecondHistory);
            TotalRead = storage.TotalRead;
            TotalWrite = storage.TotalWrite;
            ReadLatency = storage.ReadLatencyHistogram.CreateHistogramData();
            WriteLatency = storage.WriteLatencyHistogram.CreateHistogramData();
        }
    }
}