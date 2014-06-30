using System;
using System.Collections.Specialized;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using RavenFS.Tests.Synchronization.IO;
using Xunit;
using Raven.Json.Linq;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;

namespace RavenFS.Tests.Synchronization
{
    public class ConflictNotificationTests : RavenFsTestBase
	{
        private readonly IFilesStore sourceStore;
        private readonly IFilesStore destinationStore;

        private readonly IAsyncFilesCommands destinationClient;
        private readonly IAsyncFilesCommands sourceClient;

        public ConflictNotificationTests()
		{
            sourceStore = NewStore(0);
            sourceClient = sourceStore.AsyncFilesCommands;

            destinationStore = NewStore(1);
            destinationClient = destinationStore.AsyncFilesCommands;
		}

        [Fact]
		public async Task NotificationIsReceivedWhenConflictIsDetected()
		{
			var sourceContent = new RandomlyModifiedStream(new RandomStream(1), 0.01);
			var destinationContent = new RandomlyModifiedStream(sourceContent, 0.01);

            var sourceMetadata = new RavenJObject
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };

            var destinationMetadata = new RavenJObject
				                          {
					                          {"SomeTest-metadata", "should-be-overwritten"}
				                          };

            await destinationClient.UploadAsync("abc.txt", destinationContent, destinationMetadata);
            await sourceClient.UploadAsync("abc.txt", sourceContent, sourceMetadata);

            var notificationTask = destinationStore.Changes()
                                        .ForConflicts()
                                        .OfType<ConflictNotification>()
                                        .Where(x => x.Status == ConflictStatus.Detected)
				                        .Timeout(TimeSpan.FromSeconds(5))
				                        .Take(1)
				                        .ToTask();

			await sourceClient.Synchronization.StartAsync("abc.txt", destinationClient);

			var conflictDetected = await notificationTask;

			Assert.Equal("abc.txt", conflictDetected.FileName);
            Assert.Equal(new Uri(sourceStore.Url).Port, new Uri(conflictDetected.SourceServerUrl).Port);
		}

		public override void Dispose()
		{
            destinationClient.Dispose();
            sourceClient.Dispose();
			base.Dispose();
		}
	}
}