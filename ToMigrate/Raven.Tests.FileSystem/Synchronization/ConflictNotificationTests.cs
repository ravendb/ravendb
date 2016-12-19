using System;
using System.Collections.Specialized;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Raven.Json.Linq;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class ConflictNotificationTests : RavenFilesTestWithLogs
    {

        [Fact]
        public async Task NotificationIsReceivedWhenConflictIsDetected()
        {
            using (var sourceStore = NewStore(0) )
            using (var destinationStore = NewStore(1))
            {
                var sourceClient = sourceStore.AsyncFilesCommands;
                var destinationClient = destinationStore.AsyncFilesCommands;

                var sourceContent = new MemoryStream();
                new RandomlyModifiedStream(new RandomStream(1), 0.01).CopyTo(sourceContent);

                sourceContent.Position = 0;
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
                sourceContent.Position = 0;
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

                Assert.Equal("/abc.txt", conflictDetected.FileName);
                Assert.Equal(new Uri(sourceStore.Url).Port, new Uri(conflictDetected.SourceServerUrl).Port);
            }
        }
    }
}
