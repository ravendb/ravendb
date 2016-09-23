// -----------------------------------------------------------------------
//  <copyright file="DiskPerformanceTester.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Voron.Platform.Win32;
using Raven.Database.Extensions;
using Raven.Database.Util;
using Raven.Imports.metrics.Core;

namespace Raven.Database.DiskIO
{
    public abstract class AbstractDiskPerformanceTester : IDisposable
    {
        public const string PerformanceResultDocumentKey = "Raven/Disk/Performance";

        public const string TemporaryFileName = "data.ravendb-io-test";

        public const string TemporaryJournalFileName = "journal.ravendb-io-test";

        public abstract void TestDiskIO();

        public abstract DiskPerformanceResult Result { get; }

        public static AbstractDiskPerformanceTester ForRequest(AbstractPerformanceTestRequest ioTestRequest, Action<string> add, CancellationToken token = new CancellationToken())
        {
            var genericPerformanceRequest = ioTestRequest as GenericPerformanceTestRequest;
            if (genericPerformanceRequest != null)
            {
                return new GenericDiskPerformanceTester(genericPerformanceRequest, add, token);
            }
            var batchPerformanceRequest = ioTestRequest as BatchPerformanceTestRequest;
            if (batchPerformanceRequest != null)
            {
                return new BatchDiskPerformanceTester(batchPerformanceRequest, add, token);
            }
            throw new ArgumentException("Invalid ioTestRequest type", "ioTestRequest");
        }

        public abstract void Dispose();
        public abstract void DescribeTestParameters();
    }

    public abstract class AbstractDiskPerformanceTester<TRequest> : AbstractDiskPerformanceTester where TRequest : AbstractPerformanceTestRequest
    {
        protected readonly TRequest testRequest;

        protected readonly Action<string> onInfo;

        protected readonly CancellationTokenSource testTimerCts;

        protected readonly CancellationTokenSource linkedCts;

        protected readonly CancellationToken taskKillToken;

        protected readonly DiskPerformanceStorage dataStorage;

        protected string filePath;
        
        protected Timer secondTimer;

        protected long statCounter;

        protected long testTime;

        public override DiskPerformanceResult Result
        {
            get
            {
                return new DiskPerformanceResult(dataStorage, testTime);
            }
        }

        protected IDisposable TestTimeMeasure()
        {
            var sw = new Stopwatch();
            sw.Start();
            return new DisposableAction(() =>
            {
                sw.Stop();
                testTime = sw.ElapsedMilliseconds;
            });
        }

        protected AbstractDiskPerformanceTester(TRequest testRequest, Action<string> onInfo, CancellationToken taskKillToken = default(CancellationToken))
        {
            this.testRequest = testRequest;
            this.onInfo = onInfo;
            this.taskKillToken = taskKillToken;
            dataStorage = new DiskPerformanceStorage();
            testTimerCts = new CancellationTokenSource();
            linkedCts = CancellationTokenSource.CreateLinkedTokenSource(taskKillToken, testTimerCts.Token);
        }

        protected void PrepareTestFile(string path)
        {
            var sw = new Stopwatch();
            sw.Start();

            AssertDiskSpace(path, testRequest.FileSize);

            if (File.Exists(path))
            {
                var fInfo = new FileInfo(path);
                if (fInfo.Length < testRequest.FileSize)
                {
                    onInfo(string.Format("Expanding test file to {0}", testRequest.FileSize));
                    using (var fs = new FileStream(path, FileMode.Append, FileAccess.Write))
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
                onInfo(string.Format("Creating test file with size = {0}", SizeHelper.Humane(testRequest.FileSize)));
                using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    const int bufferSize = 4 * 1024;
                    var buffer = new byte[bufferSize];
                    var random = new Random();

                    for (long i = 0; i < testRequest.FileSize; i += bufferSize)
                    {
                        taskKillToken.ThrowIfCancellationRequested();
                        random.NextBytes(buffer);
                        fs.Write(buffer, 0, bufferSize);
                    }
                }
                var elapsed = sw.Elapsed;
                onInfo(string.Format("Test file created with size: {0} in {1}", testRequest.FileSize, elapsed));
            }
        }

        private static void AssertDiskSpace(string path, long fileSize)
        {
            var pathRoot = new DirectoryInfo(path).Root;
            var drive = new DriveInfo(pathRoot.FullName);
            if (drive != null && drive.AvailableFreeSpace / 2  < fileSize)
            {
                throw new Exception("Temporary test file size cannot exceed more than 50% of current free disk space.");
            }
        }

        protected long LongRandom(long min, long max, int mutlipleOf, Random rand)
        {
            var buf = new byte[8];
            rand.NextBytes(buf);
            var longRand = BitConverter.ToInt64(buf, 0);
            var value = (Math.Abs(longRand % (max - min)) + min);
            return value/mutlipleOf*mutlipleOf;
        }
        
        protected static void ValidateHandle(SafeFileHandle handle)
        {
            if (handle.IsInvalid)
            {
                int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open test file",
                                      new Win32Exception(lastWin32ErrorCode));
            }
        }

        public override void Dispose()
        {
            DisposeTimer();
            IOExtensions.DeleteFile(filePath);
            linkedCts.Dispose();
        }

        protected void DisposeTimer()
        {
            if (secondTimer == null)
                return;

            secondTimer.Dispose();
            secondTimer = null;
        }

    }

    public class GenericDiskPerformanceTester : AbstractDiskPerformanceTester<GenericPerformanceTestRequest>
    {

        private readonly List<Thread> threads;
        private readonly List<Random> perThreadRandom;

        public GenericDiskPerformanceTester(GenericPerformanceTestRequest testRequest, Action<string> onInfo, CancellationToken taskKillToken = new CancellationToken()) : base(testRequest, onInfo, taskKillToken)
        {
            IOExtensions.CreateDirectoryIfNotExists(testRequest.Path);
            filePath = Path.Combine(testRequest.Path, TemporaryFileName);
            threads = new List<Thread>(testRequest.ThreadCount);
            perThreadRandom = Enumerable.Range(1, testRequest.ThreadCount)
                .Select(i => testRequest.RandomSeed.HasValue ? new Random(testRequest.RandomSeed.Value) : new Random()).ToList();

            if (testRequest.Sequential && testRequest.OperationType == OperationType.Mix)
            {
                onInfo("Sequential test with mixed read/write mode is not supported. Changing to random access");
                testRequest.Sequential = false;
            }
        }

        public override void TestDiskIO()
        {
            PrepareTestFile(filePath);
            onInfo("Starting test...");
            using (TestTimeMeasure())
            {
                StartWorkers();
                onInfo("Waiting for test to complete");
                threads.ForEach(t => t.Join());
                taskKillToken.ThrowIfCancellationRequested();
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

        protected void SecondTicked(object state)
        {
            statCounter++;

            dataStorage.Update();

            if (statCounter >= testRequest.TimeToRunInSeconds)
            {
                testTimerCts.Cancel();
                DisposeTimer();
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

            using (var readHandle = Win32NativeFileMethods.CreateFile(filePath,
                                                                  Win32NativeFileAccess.GenericWrite | Win32NativeFileAccess.GenericRead,
                                                                  Win32NativeFileShare.Read | Win32NativeFileShare.Write, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  testRequest.BufferedReads ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering,
                                                                  IntPtr.Zero))

            using (var writeHandle = Win32NativeFileMethods.CreateFile(filePath,
                                                                  Win32NativeFileAccess.GenericWrite | Win32NativeFileAccess.GenericRead,
                                                                  Win32NativeFileShare.Read | Win32NativeFileShare.Write, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  testRequest.BufferedWrites ? Win32NativeFileAttributes.None : (Win32NativeFileAttributes.NoBuffering | Win32NativeFileAttributes.Write_Through),
                                                                  IntPtr.Zero))
            {
                ValidateHandle(readHandle);
                ValidateHandle(writeHandle);
                using (var readFs = new FileStream(readHandle, FileAccess.Read))
                using (var writeFs = new FileStream(readHandle, FileAccess.Write))
                {
                    var buffer = new byte[testRequest.ChunkSize];
                    var sw = new Stopwatch();
                    while (token.IsCancellationRequested == false)
                    {
                        if (random.Next(2) > 0)
                        {
                            var position = LongRandom(0, testRequest.FileSize, 4096, random);
                            sw.Restart();
                            readFs.Seek(position, SeekOrigin.Begin);
                            readFs.Read(buffer, 0, testRequest.ChunkSize);
                            dataStorage.MarkRead(testRequest.ChunkSize, sw.ElapsedMilliseconds);
                        }
                        else
                        {
                            var position = LongRandom(start, end - testRequest.ChunkSize, 4096, random);
                            random.NextBytes(buffer);
                            sw.Restart();
                            writeFs.Seek(position, SeekOrigin.Begin);
                            writeFs.Write(buffer, 0, testRequest.ChunkSize);
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
            using (var handle = Win32NativeFileMethods.CreateFile(filePath,
                                                                  Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.Write, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  testRequest.BufferedWrites ? Win32NativeFileAttributes.None : (Win32NativeFileAttributes.NoBuffering | Win32NativeFileAttributes.Write_Through),
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
            using (var handle = Win32NativeFileMethods.CreateFile(filePath,
                                                     Win32NativeFileAccess.GenericRead, Win32NativeFileShare.Read, IntPtr.Zero,
                                                     Win32NativeFileCreationDisposition.OpenExisting,
                                                     testRequest.BufferedReads ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering,
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
            using (var handle = Win32NativeFileMethods.CreateFile(filePath,
                                                                  Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.Write, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  testRequest.BufferedWrites ? Win32NativeFileAttributes.None : (Win32NativeFileAttributes.Write_Through | Win32NativeFileAttributes.NoBuffering),
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
            using (var handle = Win32NativeFileMethods.CreateFile(filePath,
                                                                  Win32NativeFileAccess.GenericRead, Win32NativeFileShare.Read, IntPtr.Zero,
                                                                  Win32NativeFileCreationDisposition.OpenExisting,
                                                                  testRequest.BufferedReads ? Win32NativeFileAttributes.None : Win32NativeFileAttributes.NoBuffering,
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

        public override void DescribeTestParameters()
        {
            var action = "read/write";
            if (testRequest.OperationType == OperationType.Write)
            {
                action = "write";
            }
            else if (testRequest.OperationType == OperationType.Read)
            {
                action = "read";
            }

            Console.WriteLine("{0} threads {1} {2} {3} {4} for {5} seconds from file {6} (size = {7} MB) in {8} kb chunks.",
                testRequest.ThreadCount, action, testRequest.BufferedReads ? "buffered reads" : "unbuffered reads", testRequest.BufferedWrites ? "buffered writes" : "unbuffered writes",
                testRequest.Sequential ? "sequential" : "random", testRequest.TimeToRunInSeconds,
                filePath, testRequest.FileSize / 1024 / 1024, testRequest.ChunkSize / 1024);
        }
    }

    public class BatchDiskPerformanceTester : AbstractDiskPerformanceTester<BatchPerformanceTestRequest>
    {
        private readonly string journalPath;

        private SafeFileHandle dataHandle;
        private SafeFileHandle journalHandle;

        private FileStream dataFs;
        private FileStream journalFs;

        public BatchDiskPerformanceTester(BatchPerformanceTestRequest testRequest, Action<string> onInfo, CancellationToken token = new CancellationToken()) : base(testRequest, onInfo, token)
        {
            IOExtensions.CreateDirectoryIfNotExists(testRequest.Path);
            filePath = Path.Combine(testRequest.Path, TemporaryFileName);
            journalPath = Path.Combine(testRequest.Path, TemporaryJournalFileName);
        }

        public override void TestDiskIO()
        {
            PrepareTestFile(filePath);
            if (File.Exists(journalPath))
            {
                File.Delete(journalPath);
            }

            using (TestTimeMeasure())
            {
                onInfo("Starting test...");
                taskKillToken.ThrowIfCancellationRequested();

                try
                {
                    secondTimer = new Timer(SecondTicked, null, 1000, 1000);

                    using (dataHandle = Win32NativeFileMethods.CreateFile(filePath,
                                                                          Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.None, IntPtr.Zero,
                                                                          Win32NativeFileCreationDisposition.OpenExisting,
                                                                          Win32NativeFileAttributes.Write_Through,
                                                                          IntPtr.Zero))
                    {
                        ValidateHandle(dataHandle);
                        using (dataFs = new FileStream(dataHandle, FileAccess.Write))
                        using (journalHandle = Win32NativeFileMethods.CreateFile(journalPath,
                                                                                 Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.None, IntPtr.Zero,
                                                                                 Win32NativeFileCreationDisposition.CreateAlways,
                                                                                 Win32NativeFileAttributes.Write_Through | Win32NativeFileAttributes.NoBuffering,
                                                                                 IntPtr.Zero))
                        {
                            ValidateHandle(journalHandle);
                            using (journalFs = new FileStream(journalHandle, FileAccess.Write))
                            {
                                MeasurePerformance();
                            }
                        }
                    }
                }
                finally
                {
                    if (File.Exists(journalPath))
                    {
                        IOExtensions.DeleteFile(journalPath);
                    }
                }
            }
        }

        private int RoundToMultipleOf(int number, int multiple)
        {
            var rounded = (int) Math.Round(number*1.0/multiple);
            return rounded*multiple;
        }

        private void MeasurePerformance()
        {
            var remainingDocuments = testRequest.NumberOfDocuments;

            var buffer = new byte[RoundToMultipleOf(4 * 1024 + testRequest.NumberOfDocumentsInBatch * testRequest.SizeOfDocuments, 4 * 1024)];

            var dataFileWriteCounter = 0;
            var random = new Random();

            var sw = new Stopwatch();

            while (remainingDocuments > 0)
            {
                taskKillToken.ThrowIfCancellationRequested();
                var documentsToProcessInCurrentBatch = Math.Min(remainingDocuments, testRequest.NumberOfDocumentsInBatch);

                // Write 4 KB + (Size of documents + number  of docs in batch - rounded to 4KB) to a "journal" file (no buffering, write through)
                int bytesToWrite = RoundToMultipleOf(4 * 1024 + documentsToProcessInCurrentBatch * testRequest.SizeOfDocuments, 4 * 1024);
                sw.Restart();
                journalFs.Write(buffer, 0, bytesToWrite);
                dataStorage.MarkWrite(bytesToWrite, sw.ElapsedMilliseconds);

                // Write 4 KB to a position in "data" file (buffering, write through)
                bytesToWrite = 4*1024;
                var dataStartPosition = LongRandom(0, dataFs.Length - 4 * 1024, 4 * 1024, random);
                dataFs.Seek(dataStartPosition, SeekOrigin.Begin);
                sw.Restart();
                dataFs.Write(buffer, 0, bytesToWrite);
                dataStorage.MarkWrite(bytesToWrite, sw.ElapsedMilliseconds);
                dataFileWriteCounter++;
                FsyncIfNeeded(dataFileWriteCounter);

                // Write (Size of documents + number  of docs in batch - rounded to 4KB) to a different position in the "data" file (buffering, write through)
                bytesToWrite = RoundToMultipleOf(documentsToProcessInCurrentBatch*testRequest.SizeOfDocuments, 4*1024);
                dataStartPosition = LongRandom(0, dataFs.Length - bytesToWrite, 4*1024, random);
                dataFs.Seek(dataStartPosition, SeekOrigin.Begin);
                sw.Restart();
                dataFs.Write(buffer, 0, bytesToWrite);
                dataStorage.MarkWrite(bytesToWrite, sw.ElapsedMilliseconds);
                dataFileWriteCounter++;
                FsyncIfNeeded(dataFileWriteCounter);

                remainingDocuments -= documentsToProcessInCurrentBatch;
                if (testRequest.WaitBetweenBatches > 0)
                {
                    Thread.Sleep(testRequest.WaitBetweenBatches);
                }
            }
        }

        private void FsyncIfNeeded(int dataFileWriteCounter)
        {
            if (dataFileWriteCounter%500 == 0)
            {
                dataFs.Flush(true);
                journalFs.Flush(true);
                Win32NativeFileMethods.FlushFileBuffers(dataHandle);
                Win32NativeFileMethods.FlushFileBuffers(journalHandle);
            }
        }

        protected void SecondTicked(object state)
        {
            statCounter++;
            dataStorage.Update();
        }

        public override void DescribeTestParameters()
        {
            throw new NotSupportedException();
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
        public long TotalTimeMs { get; private set; }

        public DiskPerformanceResult(DiskPerformanceStorage storage, long totalTimeMs)
        {
            ReadPerSecondHistory = new List<long>(storage.ReadPerSecondHistory);
            WritePerSecondHistory = new List<long>(storage.WritePerSecondHistory);
            AverageReadLatencyPerSecondHistory = new List<double>(storage.AverageReadLatencyPerSecondHistory);
            AverageWriteLatencyPerSecondHistory = new List<double>(storage.AverageWriteLatencyPerSecondHistory);
            TotalRead = storage.TotalRead;
            TotalWrite = storage.TotalWrite;
            ReadLatency = storage.ReadLatencyHistogram.CreateHistogramData();
            WriteLatency = storage.WriteLatencyHistogram.CreateHistogramData();
            TotalTimeMs = totalTimeMs;
        }
    }
}
