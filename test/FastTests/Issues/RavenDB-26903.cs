using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_26903 : NoDisposalNeeded
    {
        public RavenDB_26903(ITestOutputHelper output) : base(output)
        {
        }

        // Backups run the smuggler under AsyncHelpers.RunSyncWithSynchronization, which installs a
        // single-threaded ExclusiveSynchronizationContext on the dedicated backup thread and pumps it
        // only while the smuggler is executing. A multipart upload part is started (fire-and-forget)
        // during that run, and the final in-flight part is awaited later in DirectUploadStream.Dispose.
        //
        // If the cloud upload awaits capture that synchronization context (the default ConfigureAwait(true)),
        // the part's completion continuation is posted back to the context after its message loop has already
        // ended, so it can never run. DirectUploadStream.Dispose then blocks forever on that task, hanging the
        // dedicated backup thread.
        //
        // This test reproduces the exact lifecycle with the real AwsS3MultiPartUploader and a fake S3 client
        // (no network): a context that captures continuations but never runs them, an upload started under it,
        // and a blocking wait afterwards. It times out (fails) unless the uploader uses ConfigureAwait(false).
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void DirectUpload_multipart_upload_must_not_capture_the_backup_synchronization_context()
        {
            var client = new AsyncCompletingS3Client();
            var uploader = new AwsS3MultiPartUploader(client, bucketName: "bucket", storageClass: S3StorageClass.Standard,
                progress: null, key: "key", metadata: new Dictionary<string, string>(), cancellationToken: default);

            // Initialize() runs without a synchronization context (fast path), exactly like during backup setup.
            uploader.Initialize();

            using var stream = new MemoryStream(new byte[1024]);

            var context = new NonPumpingSynchronizationContext();
            var previous = SynchronizationContext.Current;

            Task uploadTask;
            SynchronizationContext.SetSynchronizationContext(context);
            try
            {
                // Mirrors StartUploadTask() running on the backup thread while the ExclusiveSynchronizationContext
                // is current: the upload is kicked off here but not awaited.
                uploadTask = uploader.UploadPartAsync(stream);
            }
            finally
            {
                // The smuggler finishes and the message loop ends -> the context is no longer pumped,
                // just like when RunSyncWithSynchronization returns.
                SynchronizationContext.SetSynchronizationContext(previous);
            }

            // Mirrors DirectUploadStream.Dispose blocking on the last in-flight part. If the part's continuation
            // was captured by 'context' (which is no longer pumped), the task never completes and this times out.
            var completed = uploadTask.Wait(TimeSpan.FromSeconds(10));

            Assert.True(completed,
                "The multipart upload task never completed: its continuation was captured by the backup " +
                "synchronization context and orphaned once the message loop ended. The cloud upload awaits " +
                "must use ConfigureAwait(false).");
        }

        // A synchronization context that captures posted continuations but never executes them - modeling the
        // backup's ExclusiveSynchronizationContext after its message loop has stopped pumping.
        private sealed class NonPumpingSynchronizationContext : SynchronizationContext
        {
            public override void Post(SendOrPostCallback d, object state)
            {
                // Intentionally drop the continuation: nothing pumps this context anymore.
            }

            public override void Send(SendOrPostCallback d, object state)
            {
                throw new NotSupportedException();
            }

            public override SynchronizationContext CreateCopy() => this;
        }

        // Fake S3 client whose async operations complete asynchronously on the thread pool (no network).
        // It uses ConfigureAwait(false) internally so that its OWN completion does not depend on the test's
        // non-pumping context - only the production uploader's ConfigureAwait behavior is under test.
        private sealed class AsyncCompletingS3Client : AmazonS3Client
        {
            public AsyncCompletingS3Client()
                : base(new BasicAWSCredentials("test", "test"), new AmazonS3Config { ServiceURL = "https://localhost:1" })
            {
            }

            public override async Task<InitiateMultipartUploadResponse> InitiateMultipartUploadAsync(InitiateMultipartUploadRequest request, CancellationToken cancellationToken = default)
            {
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
                return new InitiateMultipartUploadResponse { UploadId = "test-upload-id" };
            }

            public override async Task<UploadPartResponse> UploadPartAsync(UploadPartRequest request, CancellationToken cancellationToken = default)
            {
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                return new UploadPartResponse { PartNumber = 1, ETag = "\"test-etag\"" };
            }

            public override async Task<CompleteMultipartUploadResponse> CompleteMultipartUploadAsync(CompleteMultipartUploadRequest request, CancellationToken cancellationToken = default)
            {
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
                return new CompleteMultipartUploadResponse();
            }
        }
    }
}
