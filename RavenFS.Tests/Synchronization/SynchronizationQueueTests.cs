using System;
using System.Collections.Specialized;
using System.Linq;
using Raven.Client.RavenFS;
using Raven.Json.Linq;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Xunit;


namespace RavenFS.Tests.Synchronization
{
	public class SynchronizationQueueTests : StorageTest
	{
        private string Destination = string.Format("{0}/ravenfs/{1}", "http://dest", "test");
		private const string FileName = "test.txt";

        private static readonly RavenJObject EmptyETagMetadata = new RavenJObject().WithETag(Guid.Empty);

		private readonly SynchronizationQueue queue;

		public SynchronizationQueueTests()
		{
			queue = new SynchronizationQueue();
		}

		[Fact]
		public void Should_not_enqueue_synchronization_if_the_same_work_is_active()
		{
			transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, EmptyETagMetadata));

			queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));

			SynchronizationWorkItem work;

			queue.TryDequePendingSynchronization(Destination, out work);
			queue.SynchronizationStarted(work, Destination);

			// attempt to enqueue the same work
			queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));

			Assert.Equal(1, queue.Active.Count());
			Assert.Equal(0, queue.Pending.Count());
		}

		[Fact]
		public void Should_enqueue_to_pending_if_work_of_the_same_type_but_with_different_etag_is_active()
		{
			transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, EmptyETagMetadata));

			queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));

			SynchronizationWorkItem work;

			queue.TryDequePendingSynchronization(Destination, out work);
			queue.SynchronizationStarted(work, Destination);

            transactionalStorage.Batch(accessor => accessor.UpdateFileMetadata(FileName, new RavenJObject().WithETag(Guid.NewGuid())));			

			var metadataUpdateWorkItem = new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage);

			metadataUpdateWorkItem.RefreshMetadata();

			queue.EnqueueSynchronization(Destination, metadataUpdateWorkItem);

			Assert.Equal(1, queue.Active.Count());
			Assert.Equal(1, queue.Pending.Count());
		}

		[MtaFact]
		public void Should_be_only_work_with_greater_etag_in_pending_queue()
		{
			using (var sigGenerator = new SigGenerator())
			{
				transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, EmptyETagMetadata));

				queue.EnqueueSynchronization(Destination, new ContentUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage, sigGenerator));

				Assert.Equal(1, queue.Pending.Count());

				var greaterGuid = Guid.NewGuid();

				transactionalStorage.Batch(accessor => accessor.UpdateFileMetadata(FileName, new RavenJObject().WithETag(greaterGuid)));

				queue.EnqueueSynchronization(Destination, new ContentUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage, new SigGenerator()));

				Assert.Equal(1, queue.Pending.Count());
				Assert.Equal(greaterGuid, queue.Pending.ToArray()[0].FileETag);
			}
		}

		[MtaFact]
		public void Should_detect_that_different_work_is_being_perfomed()
		{
			using (var sigGenerator = new SigGenerator())
			{
				transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, EmptyETagMetadata));

				var contentUpdateWorkItem = new ContentUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage, sigGenerator);

				queue.EnqueueSynchronization(Destination, contentUpdateWorkItem);
				queue.SynchronizationStarted(contentUpdateWorkItem, Destination);

				Assert.True(queue.IsDifferentWorkForTheSameFileBeingPerformed(new RenameWorkItem(FileName, "rename.txt", "http://localhost:12345", transactionalStorage), Destination));
			}
		}

		[Fact]
		public void Should_delete_other_works_if_delete_item_enqueued()
		{
			transactionalStorage.Batch(accessor => accessor.PutFile(FileName, 0, EmptyETagMetadata));

			queue.EnqueueSynchronization(Destination, new MetadataUpdateWorkItem(FileName, "http://localhost:12345", transactionalStorage));
			queue.EnqueueSynchronization(Destination, new RenameWorkItem(FileName, "rename.txt", "http://localhost:12345", transactionalStorage));

			Assert.Equal(2, queue.Pending.Count());

			queue.EnqueueSynchronization(Destination, new DeleteWorkItem(FileName, "http://localhost:12345", transactionalStorage));

			Assert.Equal(1, queue.Pending.Count());
			Assert.Equal(SynchronizationType.Delete, queue.Pending.ToArray()[0].Type);
		}
	}
}