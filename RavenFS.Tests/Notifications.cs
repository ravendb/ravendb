using System;
using System.Collections.Specialized;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Xunit;
using Raven.Json.Linq;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;

namespace RavenFS.Tests
{
    public class Notifications : RavenFsTestBase
    {
        private readonly IFilesStore store;
        private readonly IAsyncFilesCommands client;

	    public Notifications()
	    {
            store = NewStore();
            client = store.AsyncFilesCommands;
	    }

		[Fact]
        public async Task NotificationReceivedWhenFileAdded()
        {
            var notificationTask = store.Changes().ForFolder("/")
                                                   .Timeout(TimeSpan.FromSeconds(2))
                                                   .Take(1).ToTask();

            await client.UploadAsync("abc.txt", new MemoryStream());

            var fileChange = await notificationTask;

            Assert.Equal("/abc.txt", fileChange.File);
            Assert.Equal(FileChangeAction.Add, fileChange.Action);
        }

		[Fact]
		public async Task NotificationReceivedWhenFileDeleted()
        {
            await client.UploadAsync("abc.txt", new MemoryStream());

            var notificationTask = store.Changes().ForFolder("/")
                                               .Timeout(TimeSpan.FromSeconds(2))
                                               .Take(1).ToTask();

            await client.DeleteAsync("abc.txt");

            var fileChange = await notificationTask;

            Assert.Equal("/abc.txt", fileChange.File);
            Assert.Equal(FileChangeAction.Delete, fileChange.Action);
        }

		[Fact]
		public async Task NotificationReceivedWhenFileUpdated()
        {
            await client.UploadAsync("abc.txt", new MemoryStream());

            var notificationTask = store.Changes().ForFolder("/")
                                                .Timeout(TimeSpan.FromSeconds(2))
                                                .Take(1).ToTask();

            await client.UpdateMetadataAsync("abc.txt", new RavenJObject { { "MyMetadata", "MyValue" } });

            var fileChange = await notificationTask;

            Assert.Equal("/abc.txt", fileChange.File);
            Assert.Equal(FileChangeAction.Update, fileChange.Action);
        }

		[Fact]
		public async Task NotificationsReceivedWhenFileRenamed()
        {
            await client.UploadAsync("abc.txt", new MemoryStream());

            var notificationTask = store.Changes().ForFolder("/")
                                                .Buffer(TimeSpan.FromSeconds(5))
                                                .Take(1).ToTask();

            await client.RenameAsync("abc.txt", "newName.txt");

            var fileChanges = await notificationTask;

            Assert.Equal("/abc.txt", fileChanges[0].File);
            Assert.Equal(FileChangeAction.Renaming, fileChanges[0].Action);
            Assert.Equal("/newName.txt", fileChanges[1].File);
            Assert.Equal(FileChangeAction.Renamed, fileChanges[1].Action);
        }

		[Fact]
		public async Task NotificationsAreOnlyReceivedForFilesInGivenFolder()
        {
            var notificationTask = store.Changes().ForFolder("/Folder")
                                                .Buffer(TimeSpan.FromSeconds(2))
                                                .Take(1).ToTask();

            client.UploadAsync("AnotherFolder/abc.txt", new MemoryStream()).Wait();

            var notifications = await notificationTask;

            Assert.Equal(0, notifications.Count);
        }

		[Fact]
		public async Task NotificationsIsReceivedWhenConfigIsUpdated()
        {
            var notificationTask = store.Changes().ForConfiguration()
                                               .Timeout(TimeSpan.FromSeconds(2))
                                               .Take(1).ToTask();

            await client.Configuration.SetKeyAsync("Test", new RavenJObject());

            var configChange = await notificationTask;

            Assert.Equal("Test", configChange.Name);
            Assert.Equal(ConfigurationChangeAction.Set, configChange.Action);
        }

		[Fact]
		public async Task NotificationsIsReceivedWhenConfigIsDeleted()
        {
            var notificationTask = store.Changes().ForConfiguration()
                                                .Timeout(TimeSpan.FromSeconds(2))
                                                .Take(1).ToTask();

            await client.Configuration.DeleteKeyAsync("Test");

            var configChange = await notificationTask;

            Assert.Equal("Test", configChange.Name);
            Assert.Equal(ConfigurationChangeAction.Delete, configChange.Action);
        }

		public override void Dispose()
		{
            store.Dispose();
			base.Dispose();
		}
    }
}
