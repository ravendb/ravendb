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
using Xunit.Abstractions;

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
            var uploader = new ContextCapturingMultiPartUploader();

            var parameters = new DirectUploadStream<FakeDirectUploader>.Parameters
            {
                ClientFactory = _ => new FakeDirectUploader(uploader),
                Key = "key",
                Metadata = new Dictionary<string, string>(),
                IsFullBackup = true,
                RetentionPolicyParameters = null,
                CloudUploadStatus = new UploadToS3(),
                OnBackupException = null,
                OnProgress = _ => { }
            };

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
            var uploader = new ContextCapturingMultiPartUploader();

            var parameters = new DirectUploadStream<FakeDirectUploader>.Parameters
            {
                ClientFactory = _ => new FakeDirectUploader(uploader),
                Key = "key",
                Metadata = new Dictionary<string, string>(),
                IsFullBackup = true,
                RetentionPolicyParameters = null,
                CloudUploadStatus = new UploadToS3(),
                OnBackupException = null,
                OnProgress = _ => { }
            };

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

        private sealed class DisposeTrackingDirectUploadStream : DirectUploadStream<FakeDirectUploader>
        {
            public bool DisposeAsyncCalled;
            public bool SyncDisposeCalled;

            public DisposeTrackingDirectUploadStream(Parameters parameters) : base(parameters)
            {
            }

            // large enough that writes never trigger a mid-stream upload part - keeps the test focused on dispatch
            protected override long MinOnePartUploadSizeInBytes => long.MaxValue;

            protected override void OnCompleteUpload()
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

            protected override void OnCompleteUpload()
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
