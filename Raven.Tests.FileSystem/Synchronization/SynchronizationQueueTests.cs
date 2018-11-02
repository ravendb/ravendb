using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Xunit;
using Raven.Abstractions.FileSystem;


namespace Raven.Tests.FileSystem.Synchronization
{
    public class SynchronizationQueueTests : StorageTest
    {
        private string Destination = string.Format("{0}/fs/{1}", "http://dest", "test");
        private const string FileName = "test.txt";

        private readonly SynchronizationQueue queue;

        private InMemoryRavenConfiguration configuration;

        public SynchronizationQueueTests()
        {
            configuration = new InMemoryRavenConfiguration();
            configuration.Initialize();
            queue = new SynchronizationQueue();
        }

        [Fact]
        public void Should_not_enqueue_synchronization_if_the_same_work_is_active()
        {
            transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, new RavenJObject()));

            queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));

            SynchronizationWorkItem work;

            queue.TryDequePending(Destination, out work);
            queue.SynchronizationStarted(work, Destination);

            // attempt to enqueue the same work
            queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));

            Assert.Equal(1, queue.Active.Count());
            Assert.Equal(0, queue.Pending.Count());
        }

        [Fact]
        public void Should_enqueue_to_pending_if_work_of_the_same_type_but_with_different_etag_is_active()
        {
            transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, new RavenJObject()));

            queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));

            SynchronizationWorkItem work;

            queue.TryDequePending(Destination, out work);
            queue.SynchronizationStarted(work, Destination);

            transactionalStorage.Batch(accessor => accessor.UpdateFileMetadata(FileName, new RavenJObject(), null));			

            var metadataUpdateWorkItem = new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage);

            metadataUpdateWorkItem.RefreshMetadata();

            queue.EnqueueSynchronization(Destination, metadataUpdateWorkItem);

            Assert.Equal(1, queue.Active.Count());
            Assert.Equal(1, queue.Pending.Count());
        }

        [MtaFact]
        public void We_must_not_refresh_metadata_when_adding_newer_item_to_pending_queue_because_files_must_be_processed_in_etag_order()
        {
            using (var sigGenerator = new SigGenerator())
            {
                transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, new RavenJObject()));

                queue.EnqueueSynchronization(Destination, new ContentUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage, sigGenerator, configuration));

                Assert.Equal(1, queue.Pending.Count());

                Etag newerEtag = null;

                transactionalStorage.Batch(accessor => newerEtag = accessor.UpdateFileMetadata(FileName, new RavenJObject(), null).Etag);

                queue.EnqueueSynchronization(Destination, new ContentUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage, new SigGenerator(), configuration));

                Assert.Equal(2, queue.Pending.Count());
                Assert.True(queue.Pending.Any(x => newerEtag.CompareTo(x.FileETag) == 0));
            }
        }

        [MtaFact]
        public void Should_detect_that_different_work_is_being_perfomed()
        {
            using (var sigGenerator = new SigGenerator())
            {
                transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, new RavenJObject()));

                var contentUpdateWorkItem = new ContentUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage, sigGenerator, configuration);

                queue.EnqueueSynchronization(Destination, contentUpdateWorkItem);
                queue.SynchronizationStarted(contentUpdateWorkItem, Destination);

                Assert.True(queue.IsDifferentWorkForTheSameFileBeingPerformed(new RenameWorkItem(FileName, "rename.txt", "http://localhost:12345", transactionalStorage), Destination));
            }
        }

        [Fact]
        public void Should_delete_other_works_if_delete_item_enqueued()
        {
            transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, new RavenJObject()));

            queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));
            queue.EnqueueSynchronization(Destination, new RenameWorkItem(FileName, "rename.txt", "http://localhost:12345", transactionalStorage));

            Assert.Equal(2, queue.Pending.Count());

            queue.EnqueueSynchronization(Destination, new DeleteWorkItem(FileName, "http://localhost:12345", transactionalStorage));

            Assert.Equal(1, queue.Pending.Count());
            Assert.Equal(SynchronizationType.Delete, queue.Pending.ToArray()[0].Type);
        }
    }
}
