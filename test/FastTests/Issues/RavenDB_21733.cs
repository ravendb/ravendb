using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Upload;
using Google.Cloud.Storage.V1;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using BackupConfiguration = Raven.Client.Documents.Operations.Backups.BackupConfiguration;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace FastTests.Issues
{
    public class RavenDB_21733 : NoDisposalNeeded
    {
        private const int ChunkSize = UploadObjectOptions.MinimumChunkSize;
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        public RavenDB_21733(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Parts_are_streamed_through_a_single_resumable_upload_and_completing_creates_the_object()
        {
            var client = new FakeStorageClient();
            var progress = new Progress();
            var metadata = new Dictionary<string, string> { { "Description", "Full backup" } };
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "folder/backup.ravendb-full-backup", metadata, ChunkSize, progress, CancellationToken.None);

            uploader.Initialize();

            // unaligned part sizes - the 256 KiB alignment of the resumable upload is the SDK's concern, not the caller's
            var expected = new MemoryStream();
            using (var part = new MemoryStream())
            {
                foreach (var size in new[] { 300_001, 1_000_000, 7, 2 * ChunkSize + 13 })
                {
                    var data = RandomBytes(size);
                    expected.Write(data);

                    part.SetLength(0);
                    part.Write(data);
                    part.Position = 0;

                    await uploader.UploadPartAsync(part).WaitAsync(Timeout);

                    // the DirectUploadStream reuses the part buffer as soon as the part task completes
                    part.Position = 0;
                    part.Write(new byte[size]);
                }
            }

            Assert.False(client.Finalized, "the object must not be created before the upload is completed");

            await uploader.CompleteUploadAsync().WaitAsync(Timeout);

            Assert.True(client.SawEndOfStream);
            Assert.True(client.Finalized);
            Assert.Equal(1, client.UploadCalls);
            Assert.Equal(expected.ToArray(), client.Received.ToArray());

            Assert.Equal("bucket", client.Destination.Bucket);
            Assert.Equal("folder/backup.ravendb-full-backup", client.Destination.Name);
            Assert.Equal("application/octet-stream", client.Destination.ContentType);
            Assert.Equal("Full backup", client.Destination.Metadata["Description"]);
            Assert.Equal(ChunkSize, client.Options.ChunkSize);
            Assert.Equal(UploadValidationMode.RejectAndThrow, client.Options.UploadValidationMode);

            // the uploader reports the uploaded bytes, the final state is set by the DirectUploadStream
            Assert.Equal(expected.Length, progress.UploadProgress.UploadedInBytes);
            Assert.Equal(UploadState.Uploading, progress.UploadProgress.UploadState);
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Aborted_upload_never_reaches_the_end_of_the_stream_so_no_object_is_created(bool async)
        {
            var client = new FakeStorageClient();
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "key", new Dictionary<string, string>(), ChunkSize, progress: null, CancellationToken.None);

            uploader.Initialize();

            using (var part = new MemoryStream(RandomBytes(3 * ChunkSize)))
                await uploader.UploadPartAsync(part).WaitAsync(Timeout);

            if (async)
                await uploader.AbortAsync().WaitAsync(Timeout);
            else
                await Task.Run(uploader.Abort).WaitAsync(Timeout);

            Assert.False(client.SawEndOfStream, "an aborted upload must never end the stream cleanly - the SDK would create a truncated object");
            Assert.False(client.Finalized);
            Assert.IsAssignableFrom<OperationCanceledException>(client.ReadFailure);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Abort_before_initialize_is_a_no_op()
        {
            var client = new FakeStorageClient();
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "key", new Dictionary<string, string>(), ChunkSize, progress: null, CancellationToken.None);

            await uploader.AbortAsync().WaitAsync(Timeout);

            Assert.Equal(0, client.UploadCalls);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Upload_failure_surfaces_from_the_part_upload_instead_of_blocking_the_writer()
        {
            // the upload fails after a while, the writer is then blocked on the back pressure of the pipe
            var client = new FakeStorageClient { Failure = new IOException("cloud is down"), FailAfterBytes = ChunkSize };
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "key", new Dictionary<string, string>(), ChunkSize, progress: null, CancellationToken.None);

            uploader.Initialize();

            using (var part = new MemoryStream(RandomBytes(16 * ChunkSize)))
            {
                var error = await Assert.ThrowsAsync<IOException>(() => uploader.UploadPartAsync(part).WaitAsync(Timeout));
                Assert.Equal("cloud is down", error.Message);
            }

            using (var part = new MemoryStream(RandomBytes(1024)))
                await Assert.ThrowsAsync<IOException>(() => uploader.UploadPartAsync(part).WaitAsync(Timeout));

            await Assert.ThrowsAsync<IOException>(() => uploader.CompleteUploadAsync().WaitAsync(Timeout));

            // this is what the DirectUploadStream does after a failure, it must not throw
            await uploader.AbortAsync().WaitAsync(Timeout);

            Assert.False(client.Finalized);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Upload_failure_surfaces_from_completing_the_upload()
        {
            // e.g. a rejected CRC32C: all of the data was consumed, the last request fails
            var client = new FakeStorageClient { FailOnFinalize = new IOException("hash mismatch") };
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "key", new Dictionary<string, string>(), ChunkSize, progress: null, CancellationToken.None);

            uploader.Initialize();

            using (var part = new MemoryStream(RandomBytes(ChunkSize + 1)))
                await uploader.UploadPartAsync(part).WaitAsync(Timeout);

            var error = await Assert.ThrowsAsync<IOException>(() => uploader.CompleteUploadAsync().WaitAsync(Timeout));
            Assert.Equal("hash mismatch", error.Message);

            await uploader.AbortAsync().WaitAsync(Timeout);
        }

        // the DirectUploadStream aborts without waiting for the part that is in flight. that part might be blocked on the back pressure
        // of the pipe (the network is slower than the backup), the abort must release it and wait for it to finish
        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Abort_releases_a_part_that_waits_for_the_upload_to_consume_it(bool async)
        {
            var client = new FakeStorageClient { StallUntilCancelled = true };
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "key", new Dictionary<string, string>(), ChunkSize, progress: null, CancellationToken.None);

            uploader.Initialize();

            using var part = new MemoryStream(RandomBytes(16 * ChunkSize));
            var partTask = uploader.UploadPartAsync(part);

            await Task.Delay(100);
            Assert.False(partTask.IsCompleted, "the part should wait for the stalled upload to consume the pipe");

            if (async)
                await uploader.AbortAsync().WaitAsync(Timeout);
            else
                await Task.Run(uploader.Abort).WaitAsync(Timeout);

            Assert.True(partTask.IsCompleted, "the abort must wait for the part that was in flight");
            Assert.False(partTask.IsCompletedSuccessfully);
            Assert.False(client.SawEndOfStream);
            Assert.False(client.Finalized);
        }

        // cancelling the backup task cancels the upload: the waiting part fails instead of hanging, and the abort that follows is clean
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Cancelling_the_backup_fails_the_part_upload_and_does_not_create_the_object()
        {
            using var cts = new CancellationTokenSource();
            var client = new FakeStorageClient { StallUntilCancelled = true };
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "key", new Dictionary<string, string>(), ChunkSize, progress: null, cts.Token);

            uploader.Initialize();

            using var part = new MemoryStream(RandomBytes(16 * ChunkSize));
            var partTask = uploader.UploadPartAsync(part);

            await Task.Delay(100);
            Assert.False(partTask.IsCompleted, "the part should wait for the stalled upload to consume the pipe");

            await cts.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => partTask.WaitAsync(Timeout));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => uploader.CompleteUploadAsync().WaitAsync(Timeout));

            await uploader.AbortAsync().WaitAsync(Timeout);

            Assert.False(client.SawEndOfStream);
            Assert.False(client.Finalized);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void Chunk_size_must_be_a_multiple_of_256_KiB()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new GoogleCloudMultiPartUploader(new FakeStorageClient(), "bucket", "key", new Dictionary<string, string>(), ChunkSize + 1, progress: null, CancellationToken.None));

            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new GoogleCloudMultiPartUploader(new FakeStorageClient(), "bucket", "key", new Dictionary<string, string>(), 0, progress: null, CancellationToken.None));
        }

        // RavenDB-26903: the part upload is started from the backup thread while its single-threaded synchronization context
        // is current, and it is waited for after that context stopped pumping. It must not depend on that context.
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void Upload_must_not_capture_the_backup_synchronization_context()
        {
            var client = new FakeStorageClient();
            var uploader = new GoogleCloudMultiPartUploader(client, "bucket", "key", new Dictionary<string, string>(), ChunkSize, progress: null, CancellationToken.None);

            using var part = new MemoryStream(RandomBytes(8 * ChunkSize));

            var previous = SynchronizationContext.Current;
            Task uploadTask;
            SynchronizationContext.SetSynchronizationContext(new NonPumpingSynchronizationContext());
            try
            {
                uploader.Initialize();
                uploadTask = uploader.UploadPartAsync(part);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previous);
            }

            Assert.True(uploadTask.Wait(Timeout), "the part upload never completed: its continuation was captured by the backup synchronization context");
            Assert.True(Task.Run(uploader.CompleteUpload).Wait(Timeout), "completing the upload never finished");
            Assert.True(client.Finalized);
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(true)]
        [InlineData(false)]
        public void DirectUploadStream_uploads_the_whole_backup_through_the_Google_Cloud_uploader(bool async)
        {
            var client = new FakeStorageClient();
            var status = new UploadToGoogleCloud();
            var expected = new MemoryStream();

            void WriteBackup(Stream stream)
            {
                for (var i = 0; i < 40; i++)
                {
                    var data = RandomBytes(17_000 + i);
                    expected.Write(data);
                    stream.Write(data, 0, data.Length);
                }
            }

            if (async)
            {
                // the document backup path: async writes and an async dispose, inside the backup's pump
                RunInsidePump(async () =>
                {
                    var stream = new TestGoogleCloudDirectUploadStream(CreateParameters(client, status));
                    for (var i = 0; i < 40; i++)
                    {
                        var data = RandomBytes(17_000 + i);
                        expected.Write(data);
                        await stream.WriteAsync(data, 0, data.Length, CancellationToken.None);
                    }

                    await stream.DisposeAsync();
                });
            }
            else
            {
                // the snapshot path: sync writes and a sync dispose
                RunWithTimeout(() =>
                {
                    var stream = new TestGoogleCloudDirectUploadStream(CreateParameters(client, status));
                    WriteBackup(stream);
                    stream.Dispose();
                });
            }

            Assert.True(client.Finalized);
            Assert.Equal(expected.ToArray(), client.Received.ToArray());
            Assert.Equal(UploadState.Done, status.UploadProgress.UploadState);
            Assert.Equal(expected.Length, status.UploadProgress.UploadedInBytes);
            Assert.False(status.Skipped);
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(true)]
        [InlineData(false)]
        public void Failed_backup_aborts_the_Google_Cloud_upload_without_creating_an_object(bool async)
        {
            var client = new FakeStorageClient();
            var status = new UploadToGoogleCloud();
            Action onBackupException = null;
            var parameters = CreateParameters(client, status, registerOnBackupException: handler => onBackupException += handler);

            if (async)
            {
                RunInsidePump(async () =>
                {
                    var stream = new TestGoogleCloudDirectUploadStream(parameters);
                    var data = RandomBytes(200_000);
                    await stream.WriteAsync(data, 0, data.Length, CancellationToken.None);

                    // what BackupTask.CreateBackup does from its catch block
                    onBackupException?.Invoke();

                    await stream.DisposeAsync();
                });
            }
            else
            {
                RunWithTimeout(() =>
                {
                    var stream = new TestGoogleCloudDirectUploadStream(parameters);
                    var data = RandomBytes(200_000);
                    stream.Write(data, 0, data.Length);

                    onBackupException?.Invoke();

                    stream.Dispose();
                });
            }

            Assert.False(client.SawEndOfStream);
            Assert.False(client.Finalized);
            Assert.Equal(UploadState.Aborted, status.UploadProgress.UploadState);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void Google_Cloud_is_a_supported_direct_upload_destination()
        {
            var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
            configuration.Initialize();

            var googleCloudSettings = new GoogleCloudSettings { BucketName = "bucket", GoogleCredentialsJson = "{}" };

            var googleCloudOnly = new PeriodicBackupConfiguration
            {
                BackupUploadMode = BackupUploadMode.DirectUpload,
                GoogleCloudSettings = googleCloudSettings
            };

            Assert.Equal(BackupConfiguration.BackupDestination.GoogleCloud,
                BackupConfigurationHelper.GetBackupDestinationForDirectUpload(backupToLocalFolder: false, googleCloudOnly, configuration.Backup));

            var googleCloudAndS3 = new PeriodicBackupConfiguration
            {
                BackupUploadMode = BackupUploadMode.DirectUpload,
                GoogleCloudSettings = googleCloudSettings,
                S3Settings = new S3Settings { BucketName = "bucket", AwsRegionName = "us-east-1" }
            };

            var e = Assert.Throws<NotSupportedException>(() =>
                BackupConfigurationHelper.GetBackupDestinationForDirectUpload(backupToLocalFolder: false, googleCloudAndS3, configuration.Backup));
            Assert.Contains("more than one destination", e.Message);
        }

        private static DirectUploadStream<FakeGoogleCloudDirectUploader>.Parameters CreateParameters(FakeStorageClient client, CloudUploadStatus status,
            Action<Action> registerOnBackupException = null) =>
            new()
            {
                ClientFactory = progress => new FakeGoogleCloudDirectUploader(client, progress),
                Key = "key",
                Metadata = new Dictionary<string, string>(),
                IsFullBackup = true,
                RetentionPolicyParameters = null,
                CloudUploadStatus = status,
                RegisterOnBackupException = registerOnBackupException,
                OnProgress = _ => { }
            };

        private static byte[] RandomBytes(int size)
        {
            var bytes = new byte[size];
            Random.Shared.NextBytes(bytes);
            return bytes;
        }

        private static void RunWithTimeout(Action action)
        {
            var task = Task.Run(action);
            Assert.True(task.Wait(Timeout), "the operation did not complete in time");
        }

        private static void RunInsidePump(Func<Task> action)
        {
            var pump = new PumpingSynchronizationContext();
            var task = Task.Run(() => pump.Run(action));
            Assert.True(task.Wait(Timeout), "the operation did not complete in time inside the backup pump");
        }

        // a StorageClient whose upload consumes the source stream like the SDK's resumable upload does: it reads until the end of
        // the stream and only then "creates" the object
        private sealed class FakeStorageClient : StorageClient
        {
            public readonly MemoryStream Received = new();
            public Object Destination;
            public UploadObjectOptions Options;
            public int UploadCalls;
            public bool SawEndOfStream;
            public bool Finalized;
            public Exception ReadFailure;

            public Exception Failure;
            public long FailAfterBytes;
            public Exception FailOnFinalize;

            // a stalled network: nothing is consumed until the upload is cancelled
            public bool StallUntilCancelled;

            public override async Task<Object> UploadObjectAsync(Object destination, Stream source, UploadObjectOptions options = null,
                CancellationToken cancellationToken = default, IProgress<IUploadProgress> progress = null)
            {
                Interlocked.Increment(ref UploadCalls);
                Destination = destination;
                Options = options;

                var buffer = new byte[64 * 1024];
                try
                {
                    if (StallUntilCancelled)
                        await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken).ConfigureAwait(false);

                    while (true)
                    {
                        var read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
                        if (read == 0)
                        {
                            SawEndOfStream = true;
                            break;
                        }

                        Received.Write(buffer, 0, read);
                        progress?.Report(new FakeUploadProgress(UploadStatus.Uploading, Received.Length));

                        if (Failure != null && Received.Length >= FailAfterBytes)
                            throw Failure;

                        // simulate the network
                        await Task.Delay(1, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception e) when (e != Failure)
                {
                    ReadFailure = e;
                    throw;
                }

                await Task.Delay(10, cancellationToken).ConfigureAwait(false);

                if (FailOnFinalize != null)
                    throw FailOnFinalize;

                Finalized = true;
                progress?.Report(new FakeUploadProgress(UploadStatus.Completed, Received.Length));

                return new Object { Bucket = destination.Bucket, Name = destination.Name, Size = (ulong)Received.Length };
            }
        }

        private sealed class FakeUploadProgress : IUploadProgress
        {
            public FakeUploadProgress(UploadStatus status, long bytesSent)
            {
                Status = status;
                BytesSent = bytesSent;
            }

            public UploadStatus Status { get; }

            public long BytesSent { get; }

            public Exception Exception => null;
        }

        private sealed class FakeGoogleCloudDirectUploader : IDirectUploader
        {
            private readonly FakeStorageClient _client;
            private readonly Progress _progress;

            public FakeGoogleCloudDirectUploader(FakeStorageClient client, Progress progress)
            {
                _client = client;
                _progress = progress;
            }

            public IMultiPartUploader GetUploader(string key, Dictionary<string, string> metadata) =>
                new GoogleCloudMultiPartUploader(_client, "bucket", key, metadata, ChunkSize, _progress, CancellationToken.None);

            public void Dispose()
            {
            }
        }

        private sealed class TestGoogleCloudDirectUploadStream : DirectUploadStream<FakeGoogleCloudDirectUploader>
        {
            public TestGoogleCloudDirectUploadStream(Parameters parameters) : base(parameters)
            {
            }

            // small enough to upload many parts
            protected override long MinOnePartUploadSizeInBytes => 64 * 1024;

            protected override void OnCompleteUploadInternal()
            {
            }
        }

        // models the backup's synchronization context once its message loop has stopped: continuations posted to it never run
        private sealed class NonPumpingSynchronizationContext : SynchronizationContext
        {
            public override void Post(SendOrPostCallback d, object state)
            {
            }

            public override void Send(SendOrPostCallback d, object state) => throw new NotSupportedException();

            public override SynchronizationContext CreateCopy() => this;
        }

        // a minimal reproduction of AsyncHelpers.ExclusiveSynchronizationContext's message loop (see RavenDB-26912)
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
