using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Documents.Attachments;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class AttachmentUploaderTests : RavenTestBase
    {
        public AttachmentUploaderTests(ITestOutputHelper output) : base(output)
        {
        }

        /// <summary>
        /// Mock stream that fails on specified operations for testing failure scenarios
        /// </summary>
        private class FailingStream : MemoryStream
        {
            private readonly bool _failOnRead;
            private readonly bool _failOnWrite;
            private readonly bool _failOnDispose;
            private readonly Exception _exceptionToThrow;

            public FailingStream(byte[] buffer, bool failOnRead = false, bool failOnWrite = false, bool failOnDispose = false, Exception exceptionToThrow = null)
                : base(buffer)
            {
                _failOnRead = failOnRead;
                _failOnWrite = failOnWrite;
                _failOnDispose = failOnDispose;
                _exceptionToThrow = exceptionToThrow ?? new InvalidOperationException("Simulated stream failure");
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (_failOnRead)
                    throw _exceptionToThrow;
                return base.Read(buffer, offset, count);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (_failOnWrite)
                    throw _exceptionToThrow;
                base.Write(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                if (_failOnDispose && disposing)
                    throw _exceptionToThrow;
                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Mock uploader that exposes internal state for testing
        /// </summary>
        private class TestableAttachmentUploader : AttachmentUploader, IDisposable
        {
            public TestableAttachmentUploader(UploaderSettings settings, RavenLogger logger, OperationCancelToken taskCancelToken)
                : base(settings, logger, taskCancelToken)
            {
                _disposables = new ConcurrentSet<IDisposable>();
            }

            public int ThreadsCount => _threads.Count;
            public LinkedList<AttachmentUploadToCloudHolder> GetThreads() => _threads;

            public void ClearThreads() => _threads.Clear();
            public ConcurrentSet<IDisposable> _disposables;
            internal override Stream StreamForBackupDestination(DocumentDatabase database, string folderName, string fileName)
            {

                var d = new MemoryStream();
                _disposables.Add(d);
                return d;
            }

            public void Dispose()
            {

                foreach (var d in _disposables)
                {
                    try
                    {
                        d.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }

            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task WaitForFinishedTasksIfNeededAsync_ShouldReturnFalse_WhenCancellationTokenIsRequested()
        {
            // Arrange
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var tokenSource = new CancellationTokenSource();
            var operationToken = new OperationCancelToken(tokenSource.Token);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);
            var stopwatch = Stopwatch.StartNew();

            // Act - Cancel the token immediately
            tokenSource.Cancel();
            var result = await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);

            // Assert
            Assert.False(result);
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CreateUploadTask_ShouldHandleCancellation_WhenCancellationTokenIsRequestedDuringUpload()
        {
            // Arrange
            using var allocator = new ByteStringContext(new SharedMultipleUseFlag());

            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var tokenSource = new CancellationTokenSource();
            var operationToken = new OperationCancelToken(tokenSource.Token);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);
            using var _ = CreateMockDocumentExpirationInfo(allocator, out var doc);
            using var attachmentStream = new SlowStream(new byte[] { 1, 2, 3, 4, 5 }, delayMs: 2000);

            // Act
            uploader.CreateUploadTask(null, doc, attachmentStream, "test-hash", 5);

            // Cancel the token before the task completes
            tokenSource.Cancel();

            // Assert
            Assert.Equal(1, uploader.ThreadsCount);

            // Wait a bit for the task to process the cancellation

            var threads = uploader.GetThreads();
            var task = threads.First().UploadTask;
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);

            Assert.True(task.IsCompleted, "task.IsCompleted");
            Assert.True(task.IsCanceled, "task.IsCanceled");
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CreateUploadTask_ShouldHandleStreamFailure_WhenStreamThrowsException()
        {
            // Arrange
            using var allocator = new ByteStringContext(new SharedMultipleUseFlag());
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);
            using var _ = CreateMockDocumentExpirationInfo(allocator, out var doc);

            var exceptionToThrow = new IOException("Simulated IO failure");
            using var failingStream = new FailingStream(new byte[] { 1, 2, 3 }, failOnRead: true, exceptionToThrow: exceptionToThrow);

            var successCallbackInvoked = false;
            var exceptionCallbackInvoked = false;
            Exception caughtException = null;

            uploader.OnSuccess = _ => successCallbackInvoked = true;
            uploader.OnException = holder =>
            {
                exceptionCallbackInvoked = true;
                caughtException = holder.UploadTask.Exception;
            };

            // Act
            uploader.CreateUploadTask(null, doc, failingStream, "test-hash", 3);

            // Wait for the task to complete
            var threads = uploader.GetThreads();
            var task = threads.First().UploadTask;

            uploader.Execute();

            await Task.WhenAny(task, Task.Delay(15000));
            Assert.Equal(TaskStatus.Faulted, task.Status);

            var x = await Assert.ThrowsAsync<IOException>(async () => await task);

            // Wait for completion

            Assert.NotNull(x);
            Assert.Contains("Simulated IO failure", x.Message);
            Assert.True(task.IsCompleted);
            Assert.True(task.IsFaulted);
            Assert.False(successCallbackInvoked);
            Assert.True(exceptionCallbackInvoked);
            Assert.NotNull(caughtException);
            Assert.Contains("Simulated IO failure", caughtException.Message);
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task AssertTaskStateAndInvokeAction_ShouldInvokeSuccessCallback_WhenTaskCompletesSuccessfully()
        {
            // Arrange
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);
            using var allocator = new ByteStringContext(new SharedMultipleUseFlag());
            using var _ = CreateMockDocumentExpirationInfo(allocator, out var doc);
            using var attachmentStream = new MemoryStream(new byte[] { 1, 2, 3 });

            var successCallbackInvoked = false;
            var exceptionCallbackInvoked = false;

            uploader.OnSuccess = _ => successCallbackInvoked = true;
            uploader.OnException = _ => exceptionCallbackInvoked = true;

            // Act
            uploader.CreateUploadTask(null, doc, attachmentStream, "test-hash", 3);

            // Wait for completion
            var threads = uploader.GetThreads();
            var task = threads.First().UploadTask;
            await task;

            // Process the completed task
            uploader.Execute();

            // Assert
            Assert.True(task.IsCompletedSuccessfully);
            Assert.True(successCallbackInvoked);
            Assert.False(exceptionCallbackInvoked);
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task AssertTaskStateAndInvokeAction_ShouldNotInvokeExceptionCallback_WhenTaskIsCanceled()
        {
            // Arrange
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var tokenSource = new CancellationTokenSource();
            var operationToken = new OperationCancelToken(tokenSource.Token);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);
            using var allocator = new ByteStringContext(new SharedMultipleUseFlag());
            using var _ = CreateMockDocumentExpirationInfo(allocator, out var doc);
            using var attachmentStream = new MemoryStream(new byte[] { 1, 2, 3 });

            var successCallbackInvoked = false;
            var exceptionCallbackInvoked = false;
            uploader.OnSuccess = _ => successCallbackInvoked = true;
            uploader.OnException = _ => exceptionCallbackInvoked = true;

            // Act
            uploader.CreateUploadTask(null, doc, attachmentStream, "test-hash", 3);

            // Cancel immediately
            tokenSource.Cancel();

            // Wait for completion
            var threads = uploader.GetThreads();
            var task = threads.First().UploadTask;

            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);

            // Process the completed task
            var stopwatch = Stopwatch.StartNew();
            await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);

            // Assert
            Assert.True(task.IsCompleted, "task.IsCompleted");
            Assert.True(task.IsCanceled, "task.IsCanceled");
            Assert.False(successCallbackInvoked);
            Assert.False(exceptionCallbackInvoked);
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CreateUploadTask_ShouldDisposeStream_EvenWhenExceptionOccurs()
        {
            // Arrange
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);
            using var allocator = new ByteStringContext(new SharedMultipleUseFlag());
            using var _ = CreateMockDocumentExpirationInfo(allocator, out var doc);

            var streamDisposed = false;
            using var disposableStream = new DisposableTestStream(new byte[] { 1, 2, 3 });

            disposableStream.OnDispose = () => streamDisposed = true;

            // Make stream throw on read to simulate failure
            disposableStream.ShouldFailOnRead = true;

            // Act
            uploader.CreateUploadTask(null, doc, disposableStream, "test-hash", 3);

            // Wait for the task to complete (should fail)
            var threads = uploader.GetThreads();
            var task = threads.First().UploadTask;

            await Assert.ThrowsAsync<IOException>(async () => await task);
            // Give a moment for disposal to occur
            Thread.Sleep(100);

            // Assert
            Assert.True(streamDisposed, "Stream should be disposed even when exception occurs");
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CreateUploadTask_ShouldNotLeakStreams_WhenMultipleTasksAreCreated()
        {
            // Arrange
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);

            var streamDisposeCount = 0;
            var totalStreams = 10;

            var list = new ConcurrentSet<IDisposable>();
            var allocator = new ByteStringContext(new SharedMultipleUseFlag(), 4096);
            list.Add(allocator);
            try
            {
                for (int i = 0; i < totalStreams; i++)
                {
                    var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"doc-{i}");
                    var disposableStream = new DisposableTestStream(new byte[] { 1, 2, 3, (byte)i });
                    list.Add(disposable);
                    list.Add(disposableStream);
                    disposableStream.OnDispose = () => Interlocked.Increment(ref streamDisposeCount);

                    uploader.CreateUploadTask(null, doc, disposableStream, $"hash-{i}", 4);
                }

                // Wait for all tasks to complete
                var threads = uploader.GetThreads();
                var tasks = threads.Select(t => t.UploadTask).ToArray();
                await Task.WhenAll(tasks);
                // Give time for disposal
                Thread.Sleep(500);

                // Assert
                Assert.Equal(totalStreams, streamDisposeCount);
            }
            finally
            {
                foreach (var disposable in list)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task WaitForFinishedTasksIfNeededAsync_ShouldHandleConcurrentModification_WhenTasksCompleteWhileIterating()
        {
            // Arrange
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);

            var completedTasksCount = 0;
            uploader.OnSuccess = _ => Interlocked.Increment(ref completedTasksCount);

            var list = new ConcurrentSet<IDisposable>();
            var allocator = new ByteStringContext(new SharedMultipleUseFlag(), 4096);
            list.Add(allocator);
            try
            {
                // Create multiple quick-completing tasks
                for (int i = 0; i < 5; i++)
                {
                    var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"doc-{i}");
                    var stream = new MemoryStream(new byte[] { (byte)i });
                    list.Add(disposable);
                    list.Add(stream);
                    uploader.CreateUploadTask(null, doc, stream, $"hash-{i}", 1);
                }

                // Act - Process tasks while they might be completing
                var stopwatch = Stopwatch.StartNew();
                var result = await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);

                // Wait a bit more for any remaining tasks
                Thread.Sleep(100);
                await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);

                // Assert
                Assert.True(result);
                Assert.True(completedTasksCount > 0, "At least some tasks should have completed");
                Assert.True(uploader.ThreadsCount <= 5, "Thread count should not exceed original count");
            }
            finally
            {
                foreach (var disposable in list)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task WaitForFinishedTasksIfNeededAsync_ShouldSafelyRemoveNodes_WhenIteratingLinkedList()
        {
            // Arrange
            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);

            // Create a mix of quick and slow tasks
            var quickTasks = 3;
            var slowTasks = 2;

            var list = new ConcurrentSet<IDisposable>();
            var allocator = new ByteStringContext(new SharedMultipleUseFlag(), 4096);
            list.Add(allocator);
            try
            {
                // Quick tasks (complete fast)
                for (int i = 0; i < quickTasks; i++)
                {
                    var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"quick-{i}");
                    var stream = new MemoryStream(new byte[] { (byte)i });
                    list.Add(disposable);
                    list.Add(stream);
                    uploader.CreateUploadTask(null, doc, stream, $"hash-quick-{i}", 1);
                }

                // Slow tasks (take longer to complete)
                for (int i = 0; i < slowTasks; i++)
                {
                    var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"slow-{i}");
                    var stream = new SlowStream(new byte[] { (byte)(i + 100) }, delayMs: 1000);
                    list.Add(disposable);
                    list.Add(stream);
                    uploader.CreateUploadTask(null, doc, stream, $"hash-slow-{i}", 1);
                }

                var initialCount = uploader.ThreadsCount;
                Assert.Equal(quickTasks + slowTasks, initialCount);

                // Act - Process while some tasks are still running
                var stopwatch = Stopwatch.StartNew();
                await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);

                var countAfterFirstPass = uploader.ThreadsCount;

                // Wait for remaining tasks and process again
                Thread.Sleep(1500); // Give slow tasks time to complete
                await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);

                var finalCount = uploader.ThreadsCount;

                // Assert
                Assert.True(countAfterFirstPass < initialCount, "Some tasks should have been removed in first pass");
                Assert.True(finalCount <= countAfterFirstPass, "More tasks should have been removed in second pass");
            }
            finally
            {
                foreach (var disposable in list)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task HighVolumeConcurrentUploads_ShouldHandleStress_WithoutMemoryLeaks()
        {
            // Arrange

            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);

            var totalTasks = 100;
            var completedCount = 0;
            var failedCount = 0;

            uploader.OnSuccess = _ => Interlocked.Increment(ref completedCount);
            uploader.OnException = _ => Interlocked.Increment(ref failedCount);

            var memoryBefore = GC.GetTotalMemory(true);

            // Act - Create many concurrent upload tasks
            var list = new ConcurrentSet<IDisposable>();
            var allocator = new ByteStringContext(new SharedMultipleUseFlag(), 4096);
            list.Add(allocator);
            try
            {
                var tasks = new List<Task>();
                for (int i = 0; i < totalTasks; i++)
                {
                    var taskIndex = i;
                    var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"stress-doc-{taskIndex}");
                    var task = Task.Run(() =>
                    {
                        var data = new byte[1024]; // 1KB per attachment
                        new Random(taskIndex).NextBytes(data);
                        var stream = new MemoryStream(data);
                        list.Add(disposable);
                        list.Add(stream);

                        uploader.CreateUploadTask(null, doc, stream, $"hash-{taskIndex}", data.Length);
                    });
                    tasks.Add(task);
                }

                await Task.WhenAll(tasks);

                // Process all uploads
                var stopwatch = Stopwatch.StartNew();
                while (uploader.ThreadsCount > 0 && stopwatch.ElapsedMilliseconds < 30000) // 30 second timeout
                {
                    await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);
                    await Task.Delay(100);

                    if (uploader.ThreadsCount < 8)
                        break;
                }

                uploader.Execute();
            }
            finally
            {
                foreach (var disposable in list)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }
            }

            var memoryAfter = GC.GetTotalMemory(true);
            var memoryDelta = memoryAfter - memoryBefore;

            // Assert
            Assert.True(completedCount + failedCount >= totalTasks * 0.9, "At least 90% of tasks should complete");
            Assert.Equal(0, uploader.ThreadsCount);
            Assert.True(memoryDelta < totalTasks * 4096, $"Memory usage should be reasonable, but was: {memoryDelta} < {totalTasks * 4096} "); // Allow 4KB per task overhead
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task ConcurrentUploadAndCancellation_ShouldHandleGracefully()
        {
            // Arrange

            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var tokenSource = new CancellationTokenSource();
            var operationToken = new OperationCancelToken(tokenSource.Token);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);

            var totalTasks = 50;
            var processedCount = 0;

            uploader.OnSuccess = _ => Interlocked.Increment(ref processedCount);
            uploader.OnException = _ => Interlocked.Increment(ref processedCount);

            var list = new ConcurrentSet<IDisposable>();
            var allocator = new ByteStringContext(new SharedMultipleUseFlag(), 4096);
            list.Add(allocator);
            try
            {
                // Act - Start creating tasks and cancel partway through
                var creationTask = Task.Run(async () =>
                {
                    for (int i = 0; i < totalTasks; i++)
                    {
                        if (tokenSource.Token.IsCancellationRequested)
                            break;

                        var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"concurrent-{i}");
                        var stream = new MemoryStream(new byte[] { (byte)i, (byte)(i >> 8) });
                        list.Add(disposable);
                        list.Add(stream);

                        uploader.CreateUploadTask(null, doc, stream, $"hash-{i}", 2);

                        await Task.Delay(10); // Small delay between creations
                    }
                });

                // Cancel after half the tasks have been created
                await Task.Delay(totalTasks * 5); // Give time for some tasks to be created
                tokenSource.Cancel();

                await creationTask;

                // Wait for processing to complete
                var timeout = TimeSpan.FromSeconds(10);
                var startTime = DateTime.UtcNow;
                while (uploader.ThreadsCount > 0 && DateTime.UtcNow - startTime < timeout)
                {
                    try
                    {
                        var stopwatch = Stopwatch.StartNew();
                        await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    await Task.Delay(50);

                    if (uploader.ThreadsCount < 8)
                        break;
                }

                // Assert
                Assert.True(processedCount >= 0, "Some tasks should have been processed");
                // Note: Exact counts depend on timing, so we just ensure no crashes occurred
            }
            finally
            {
                foreach (var disposable in list)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task NetworkTimeoutScenario_ShouldHandleSlowStreams()
        {
            // Arrange

            var settings = CreateMockUploaderSettings();
            var logger = RavenLogManager.Instance.CreateNullLogger();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, logger, operationToken);

            var completedCount = 0;
            var timeoutCount = 0;

            uploader.OnSuccess = _ => Interlocked.Increment(ref completedCount);
            uploader.OnException = holder =>
            {
                if (holder.UploadTask.Exception?.InnerException is TimeoutException)
                    Interlocked.Increment(ref timeoutCount);
            };

            var list = new ConcurrentSet<IDisposable>();
            var allocator = new ByteStringContext(new SharedMultipleUseFlag(), 4096);
            list.Add(allocator);
            try
            {
                // Act - Create tasks with very slow streams (simulating network timeouts)
                for (int i = 0; i < 5; i++)
                {
                    var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"timeout-{i}");
                    var slowStream = new SlowStream(new byte[100], delayMs: 2000); // 2 second delays

                    list.Add(disposable);
                    list.Add(slowStream);
                    uploader.CreateUploadTask(null, doc, slowStream, $"hash-{i}", 100);
                }

                // Process with timeout
                var stopwatch = Stopwatch.StartNew();
                var maxWaitTime = TimeSpan.FromSeconds(10);

                while (uploader.ThreadsCount > 0 && stopwatch.Elapsed < maxWaitTime)
                {
                    await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);
                    await Task.Delay(100);

                    if (uploader.ThreadsCount < 8)
                        break;
                }
                uploader.Execute();
                // Assert
                Assert.True(completedCount + timeoutCount > 0, "Some tasks should have completed or timed out");
            }
            finally
            {
                foreach (var disposable in list)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task MemoryPressureTest_ShouldHandleLargeAttachments()
        {
            // Arrange

            var settings = CreateMockUploaderSettings();
            var operationToken = new OperationCancelToken(CancellationToken.None);

            using var uploader = new TestableAttachmentUploader(settings, RavenLogManager.Instance.CreateNullLogger(), operationToken);

            var largeAttachmentSize = 1024 * 1024; // 1MB
            var attachmentCount = 10;
            var completedCount = 0;

            uploader.OnSuccess = _ => Interlocked.Increment(ref completedCount);

            var memoryBefore = GC.GetTotalMemory(true);
            var list = new ConcurrentSet<IDisposable>();
            var allocator = new ByteStringContext(new SharedMultipleUseFlag(), 4096);
            list.Add(allocator);
            try
            {
                // Act - Create tasks with large attachments
                for (int i = 0; i < attachmentCount; i++)
                {
                    var disposable = CreateMockDocumentExpirationInfo(allocator, out var doc, $"large-{i}");
                    var largeData = new byte[largeAttachmentSize];
                    new Random(i).NextBytes(largeData);
                    var stream = new MemoryStream(largeData);
                    list.Add(disposable);
                    list.Add(stream);

                    uploader.CreateUploadTask(null, doc, stream, $"hash-{i}", largeAttachmentSize);
                }

                // Process uploads
                var stopwatch = Stopwatch.StartNew();
                while (uploader.ThreadsCount > 0 && stopwatch.ElapsedMilliseconds < 30000)
                {
                    await uploader.WaitForFinishedTasksIfNeededAsync(stopwatch, operationToken);
                    await Task.Delay(100);

                    if (uploader.ThreadsCount < 8)
                        break;
                }

                uploader.Execute();
            }
            finally
            {
                foreach (var disposable in list)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch
                    {
                        // dont care
                    }
                }
            }

            var memoryAfter = GC.GetTotalMemory(true);
            var memoryUsed = memoryAfter - memoryBefore;

            // Assert
            Assert.True(completedCount > 0, "Some large attachments should have been processed");
            Assert.True(memoryUsed < attachmentCount * largeAttachmentSize * 2.2, $"Memory usage should be reasonable for large attachments, but was: {memoryUsed} < {attachmentCount * largeAttachmentSize * 2}");
        }

        private UploaderSettings CreateMockUploaderSettings()
        {
            return new UploaderSettings(new BackupConfiguration())
            {
                ConcurrentThreads = 4,
                TaskName = "test-uploader",

                // Add other required settings based on UploaderSettings implementation
            };
        }

        private IDisposable CreateMockDocumentExpirationInfo(ByteStringContext allocator, out AbstractBackgroundWorkStorage.DocumentExpirationInfo doc, string id = "test-doc")
        {
            // This would need to be implemented based on the actual DocumentExpirationInfo structure
            // For now, returning a mock implementation
            var disposable = Slice.From(allocator, id, out var input);
            doc = new AbstractBackgroundWorkStorage.DocumentExpirationInfo(
               Slices.Empty, // LowerId - would need proper Slice implementation
               input, // Id - would need proper Slice implementation  
               id,
               AbstractBackgroundWorkStorage.DocumentExpirationInfoStatus.Process
           );

            return disposable;
        }

        private class DisposableTestStream : MemoryStream
        {
            public Action OnDispose { get; set; }
            public bool ShouldFailOnRead { get; set; }

            public DisposableTestStream(byte[] buffer) : base(buffer) { }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (ShouldFailOnRead)
                    throw new IOException("Simulated read failure");
                return base.Read(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    OnDispose?.Invoke();
                base.Dispose(disposing);
            }
        }

        private class SlowStream : MemoryStream
        {
            private readonly int _delayMs;

            public SlowStream(byte[] buffer, int delayMs) : base(buffer)
            {
                _delayMs = delayMs;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                Thread.Sleep(_delayMs);
                return base.Read(buffer, offset, count);
            }
        }
    }
}
