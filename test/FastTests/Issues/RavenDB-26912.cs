using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_26912 : NoDisposalNeeded
    {
        public RavenDB_26912(ITestOutputHelper output) : base(output)
        {
        }

        // Structural follow-up to RavenDB-26903.
        //
        // Backups run under a single-threaded synchronization-context "pump" (AsyncHelpers.RunSyncWithSynchronization).
        // While writing, DirectUploadStream starts a multipart upload part fire-and-forget (_uploadTask) whose
        // continuation is captured by that pump context. The stream must be FINALIZED (the last part awaited and the
        // upload completed) while the pump is still alive.
        //
        // The bug: the stream used to be disposed AFTER the pump had already stopped. Disposal blocked on _uploadTask
        // via a synchronous AsyncHelpers.RunSync, but that task's continuation was posted to the now-dead pump context
        // and could never run -> the backup thread deadlocked forever (and could not be aborted).
        //
        // This test reproduces the exact lifecycle with a real DirectUploadStream and a context-capturing uploader
        // (mimicking the pre-ConfigureAwait(false) SDK behavior), so it isolates the STRUCTURAL fix rather than the
        // uploader band-aid: finalizing the stream INSIDE the pump (DirectUploadStream.DisposeAsync) must not hang.
        //
        // RED  (no DisposeAsync override): 'await stream.DisposeAsync()' falls back to the synchronous Stream.Dispose,
        //      which does a nested AsyncHelpers.RunSync on _uploadTask whose continuation sits on the outer (blocked)
        //      pump -> deadlock -> the pump thread never returns -> times out.
        // GREEN (DisposeAsync override): the pending upload task is awaited directly, so the pump drains its
        //      continuation and finalization completes.
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void DirectUpload_stream_finalization_must_not_hang_when_disposed_inside_the_backup_pump()
        {
            var parameters = CreateParameters(new ContextCapturingMultiPartUploader(), new UploadToS3());

            var pump = new PumpingSynchronizationContext();

            // Run the whole stream lifetime inside the pump on a background thread, exactly like a backup does.
            var run = Task.Run(() => pump.Run(async () =>
            {
                var stream = new TestDirectUploadStream(parameters);

                // A single write past MinOnePartUploadSizeInBytes starts a fire-and-forget upload task whose
                // completion continuation is captured by this pump context.
                await stream.WriteAsync(new byte[4096], 0, 4096, CancellationToken.None);

                // Finalize INSIDE the pump. Must complete without deadlocking.
                await stream.DisposeAsync();
            }));

            var completed = run.Wait(TimeSpan.FromSeconds(20));

            Assert.True(completed,
                "DirectUploadStream finalization deadlocked: the fire-and-forget upload task's continuation was " +
                "captured by the backup synchronization context, and disposal blocked on it synchronously. " +
                "Finalization must run asynchronously inside the pump via DirectUploadStream.DisposeAsync.");
        }

        // Verifies the disposal DISPATCH on the encrypted document-backup path, with no cloud/network required.
        //
        // BackupTask.CreateBackup builds:  outputStream = GetOutputStream(directUploadStream)  (an encrypting wrapper
        // when the backup is encrypted) and finalizes via 'await outputStream.DisposeAsync()'. That must reach
        // DirectUploadStream.DisposeAsync (async), NOT the synchronous Stream.Dispose - otherwise a layer that falls
        // back to sync disposal would run the blocking Dispose inside the pump and could deadlock again.
        //
        // (The compression stream from BackupUtils.GetCompressionStream uses leaveOpen:true, so it never disposes
        // outputStream; the only disposer is CreateBackup's finally, exercised here through the encrypting layer.)
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Encrypted_backup_path_disposes_DirectUploadStream_asynchronously_not_synchronously()
        {
            var parameters = CreateParameters(new ContextCapturingMultiPartUploader(), new UploadToS3());

            var inner = new DisposeTrackingDirectUploadStream(parameters);

            // GetOutputStream(stream) wraps the DirectUploadStream in the encrypting stream when the backup is encrypted.
            var key = new byte[(int)Sodium.crypto_secretstream_xchacha20poly1305_keybytes()];
            var outputStream = new EncryptingXChaCha20Poly1305Stream(inner, key);
            outputStream.Initialize();

            await outputStream.WriteAsync(new byte[128], 0, 128);

            // Mirrors BackupTask.CreateBackup's finally.
            await outputStream.DisposeAsync();

            Assert.True(inner.DisposeAsyncCalled,
                "DirectUploadStream.DisposeAsync must be invoked through the encrypting stream on the backup path.");
            Assert.False(inner.SyncDisposeCalled,
                "The synchronous DirectUploadStream.Dispose must NOT run on the backup path - it is the blocking path that hangs.");
        }

        // Drives the abort path end to end, THROUGH the task-level delegate, because the subscription itself was silently
        // broken: Parameters used to carry the task's 'Action OnBackupException' by value and the stream did
        // 'parameters.OnBackupException += handler'. Delegates are immutable, so that only rebound the copy sitting in
        // Parameters - BackupTask's own field stayed null, its OnBackupException?.Invoke() on failure was a no-op, and
        // _abortUpload could never become true. Registration therefore has to mutate the TASK's field, which is what
        // going through FakeBackupTask (a stand-in shaped exactly like BackupTask.RegisterOnBackupException) pins here.
        //
        // The consequence of the broken wiring is what the assertions below rule out: a FAILED backup used to take the
        // success path on disposal, uploading its tail and calling CompleteUpload - which commits a TRUNCATED object at
        // the destination as a valid backup and then runs the retention policy against it, so a corrupt backup could
        // evict good ones.
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Aborted_upload_aborts_the_multipart_upload_and_never_completes_it()
        {
            var task = new FakeBackupTask();
            var uploader = new TrackingMultiPartUploader();
            var status = new UploadToS3();

            var stream = new TestDirectUploadStream(CreateParameters(uploader, status, task.RegisterOnBackupException));

            // exactly what BackupTask.CreateBackup does from its catch block
            task.SignalBackupException();

            await stream.DisposeAsync();

            Assert.True(uploader.AbortAsyncCalled,
                "the failure notification must reach the stream through the task's own delegate field and abort the in-progress upload");
            Assert.False(uploader.CompleteUploadAsyncCalled,
                "a failed backup must never be committed at the destination - that would publish a truncated object as a valid backup");
            Assert.Equal(UploadState.Aborted, status.UploadProgress.UploadState);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Successful_upload_completes_the_multipart_upload_and_marks_Done()
        {
            var uploader = new TrackingMultiPartUploader();
            var status = new UploadToS3();

            var stream = new TestDirectUploadStream(CreateParameters(uploader, status, new FakeBackupTask().RegisterOnBackupException));

            await stream.DisposeAsync();

            Assert.True(uploader.CompleteUploadAsyncCalled, "a successful upload must be completed");
            Assert.False(uploader.AbortAsyncCalled, "a successful upload must not be aborted");
            Assert.Equal(UploadState.Done, status.UploadProgress.UploadState);
        }

        // Same guarantees on the synchronous Dispose path, still used by the snapshot direct-upload backup.
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void Aborted_upload_on_sync_dispose_aborts_the_multipart_upload_and_never_completes_it()
        {
            var task = new FakeBackupTask();
            var uploader = new TrackingMultiPartUploader();
            var status = new UploadToS3();

            var stream = new TestDirectUploadStream(CreateParameters(uploader, status, task.RegisterOnBackupException));

            task.SignalBackupException();

            stream.Dispose();

            Assert.True(uploader.AbortCalled, "the in-progress multipart upload must be aborted on failure");
            Assert.False(uploader.CompleteUploadCalled, "a failed backup must never be committed at the destination");
            Assert.Equal(UploadState.Aborted, status.UploadProgress.UploadState);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void Successful_upload_on_sync_dispose_completes_the_multipart_upload_and_marks_Done()
        {
            var uploader = new TrackingMultiPartUploader();
            var status = new UploadToS3();

            var stream = new TestDirectUploadStream(CreateParameters(uploader, status, new FakeBackupTask().RegisterOnBackupException));

            stream.Dispose();

            Assert.True(uploader.CompleteUploadCalled, "a successful upload must be completed");
            Assert.False(uploader.AbortCalled, "a successful upload must not be aborted");
            Assert.Equal(UploadState.Done, status.UploadProgress.UploadState);
        }

        // Finalization can fail on its own (the tail part or CompleteUpload), with no prior backup failure to have set
        // _abortUpload. The multipart upload is then neither completed nor aborted unless disposal cleans up after
        // itself - and orphaned parts keep accruing storage costs at the destination indefinitely.
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Finalization_failure_aborts_the_multipart_upload_and_rethrows()
        {
            var uploader = new TrackingMultiPartUploader { CompleteUploadFailure = new IOException("cloud is down") };
            var status = new UploadToS3();

            var stream = new TestDirectUploadStream(CreateParameters(uploader, status));

            var error = await Assert.ThrowsAsync<IOException>(async () => await stream.DisposeAsync());

            Assert.Equal("cloud is down", error.Message);
            Assert.True(uploader.AbortAsyncCalled, "a multipart upload that could not be completed must be aborted, not left orphaned");
            Assert.Equal(UploadState.Aborted, status.UploadProgress.UploadState);
        }

        // Abort runs precisely when the destination is already unhealthy, so it is the likeliest thing to fail second.
        // It is a cleanup step and must never become the exception the caller sees, or it would routinely hide the real
        // reason the backup failed.
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Abort_failure_is_reported_but_does_not_mask_the_original_failure()
        {
            var uploader = new TrackingMultiPartUploader
            {
                CompleteUploadFailure = new IOException("cloud is down"),
                AbortFailure = new IOException("abort failed too")
            };

            var progress = new List<string>();
            var stream = new TestDirectUploadStream(CreateParameters(uploader, new UploadToS3(), onProgress: progress.Add));

            var error = await Assert.ThrowsAsync<IOException>(async () => await stream.DisposeAsync());

            Assert.Equal("cloud is down", error.Message);
            Assert.Contains(progress, message => message.Contains("Failed to abort the multipart upload"));
        }

        private static DirectUploadStream<FakeDirectUploader>.Parameters CreateParameters(
            IMultiPartUploader uploader,
            CloudUploadStatus cloudUploadStatus,
            Action<Action> registerOnBackupException = null,
            Action<string> onProgress = null) =>
            new()
            {
                ClientFactory = _ => new FakeDirectUploader(uploader),
                Key = "key",
                Metadata = new Dictionary<string, string>(),
                IsFullBackup = true,
                RetentionPolicyParameters = null,
                CloudUploadStatus = cloudUploadStatus,
                RegisterOnBackupException = registerOnBackupException,
                OnProgress = onProgress ?? (_ => { })
            };

        // Stand-in for the BackupTask side of the subscription, shaped exactly like BackupTask.RegisterOnBackupException /
        // OnBackupException?.Invoke(). Registration must reach _onBackupException here, not a copy held elsewhere.
        private sealed class FakeBackupTask
        {
            private Action _onBackupException;

            public void RegisterOnBackupException(Action handler) => _onBackupException += handler;

            public void SignalBackupException() => _onBackupException?.Invoke();
        }

        // Records which terminal operation the stream drove (complete vs abort), for both the sync and async paths, and
        // can inject failures into either.
        private sealed class TrackingMultiPartUploader : IMultiPartUploader
        {
            public bool CompleteUploadCalled;
            public bool CompleteUploadAsyncCalled;
            public bool AbortCalled;
            public bool AbortAsyncCalled;

            public Exception CompleteUploadFailure;
            public Exception AbortFailure;

            public void Initialize()
            {
            }

            public Task InitializeAsync() => Task.CompletedTask;

            public void UploadPart(Stream stream)
            {
            }

            public Task UploadPartAsync(Stream stream) => Task.CompletedTask;

            public void CompleteUpload()
            {
                CompleteUploadCalled = true;

                if (CompleteUploadFailure != null)
                    throw CompleteUploadFailure;
            }

            public Task CompleteUploadAsync()
            {
                CompleteUploadAsyncCalled = true;

                return CompleteUploadFailure != null ? Task.FromException(CompleteUploadFailure) : Task.CompletedTask;
            }

            public void Abort()
            {
                AbortCalled = true;

                if (AbortFailure != null)
                    throw AbortFailure;
            }

            public Task AbortAsync()
            {
                AbortAsyncCalled = true;

                return AbortFailure != null ? Task.FromException(AbortFailure) : Task.CompletedTask;
            }
        }

        private sealed class DisposeTrackingDirectUploadStream : DirectUploadStream<FakeDirectUploader>
        {
            public bool DisposeAsyncCalled;
            public bool SyncDisposeCalled;

            public DisposeTrackingDirectUploadStream(Parameters parameters) : base(parameters)
            {
            }

            // large enough that writes never trigger a mid-stream upload part - keeps the test focused on dispatch
            protected override long MinOnePartUploadSizeInBytes => long.MaxValue;

            protected override void OnCompleteUploadInternal()
            {
            }

            public override ValueTask DisposeAsync()
            {
                DisposeAsyncCalled = true;
                return base.DisposeAsync();
            }

            protected override void Dispose(bool disposing)
            {
                SyncDisposeCalled = true;
                base.Dispose(disposing);
            }
        }

        private sealed class TestDirectUploadStream : DirectUploadStream<FakeDirectUploader>
        {
            public TestDirectUploadStream(Parameters parameters) : base(parameters)
            {
            }

            // small enough that a single write triggers StartUploadTask()
            protected override long MinOnePartUploadSizeInBytes => 1;

            protected override void OnCompleteUploadInternal()
            {
                // no retention in the test
            }
        }

        private sealed class FakeDirectUploader : IDirectUploader
        {
            private readonly IMultiPartUploader _uploader;

            public FakeDirectUploader(IMultiPartUploader uploader)
            {
                _uploader = uploader;
            }

            public IMultiPartUploader GetUploader(string key, Dictionary<string, string> metadata) => _uploader;

            public void Dispose()
            {
            }
        }

        // An uploader whose async operations capture the current synchronization context (via Task.Yield) - i.e. they
        // do NOT use ConfigureAwait(false). This models the pre-RavenDB-26903 cloud SDK behavior and is exactly the
        // condition under which the disposal lifetime matters.
        private sealed class ContextCapturingMultiPartUploader : IMultiPartUploader
        {
            public void Initialize()
            {
            }

            public Task InitializeAsync() => Task.CompletedTask;

            public void UploadPart(Stream stream)
            {
            }

            public async Task UploadPartAsync(Stream stream)
            {
                // captures SynchronizationContext.Current (the pump) - the continuation can only run if the pump pumps it
                await Task.Yield();
            }

            public void CompleteUpload()
            {
            }

            public async Task CompleteUploadAsync()
            {
                await Task.Yield();
            }

            public void Abort()
            {
            }

            public Task AbortAsync() => Task.CompletedTask;
        }


        // A faithful, minimal reproduction of AsyncHelpers.ExclusiveSynchronizationContext's message loop
        // (RunSyncWithSynchronization is internal to Raven.Client and not visible to the test project).
        private sealed class PumpingSynchronizationContext : SynchronizationContext
        {
            private readonly AutoResetEvent _workItemsWaiting = new(false);
            private readonly ConcurrentQueue<(SendOrPostCallback Callback, object State)> _items = new();
            private bool _done;
            private Exception _error;

            public override void Post(SendOrPostCallback d, object state)
            {
                _items.Enqueue((d, state));
                _workItemsWaiting.Set();
            }

            public override void Send(SendOrPostCallback d, object state) => throw new NotSupportedException();

            public override SynchronizationContext CreateCopy() => this;

            public void Run(Func<Task> root)
            {
                var previous = Current;
                SetSynchronizationContext(this);
                try
                {
                    Post(async _ =>
                    {
                        try
                        {
                            await root();
                        }
                        catch (Exception e)
                        {
                            _error = e;
                        }
                        finally
                        {
                            _done = true;
                            _workItemsWaiting.Set();
                        }
                    }, null);

                    while (_done == false)
                    {
                        if (_items.IsEmpty)
                            _workItemsWaiting.WaitOne();

                        while (_items.TryDequeue(out var work))
                            work.Callback(work.State);
                    }
                }
                finally
                {
                    SetSynchronizationContext(previous);
                }

                if (_error != null)
                    ExceptionDispatchInfo.Capture(_error).Throw();
            }
        }
    }
}
