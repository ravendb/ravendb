using System;
using System.Collections.Specialized;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Changes;
using Xunit;
using Raven.Json.Linq;

namespace RavenFS.Tests
{
    public class Notifications : RavenFsTestBase
    {
	    private readonly RavenFileSystemClient client;

	    public Notifications()
	    {
		    client = NewClient();
	    }

		[Fact]
        public async Task NotificationReceivedWhenFileAdded()
        {
            var notificationTask = client.Notifications.ForFolder("/")
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

            var notificationTask = client.Notifications.ForFolder("/")
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

            var notificationTask = client.Notifications.ForFolder("/")
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

            var notificationTask = client.Notifications.ForFolder("/")
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
            var notificationTask = client.Notifications.ForFolder("/Folder")
                                                .Buffer(TimeSpan.FromSeconds(2))
                                                .Take(1).ToTask();

            client.UploadAsync("AnotherFolder/abc.txt", new MemoryStream()).Wait();

            var notifications = await notificationTask;

            Assert.Equal(0, notifications.Count);
        }

		[Fact]
		public async Task NotificationsIsReceivedWhenConfigIsUpdated()
        {
            var notificationTask = client.Notifications.ForConfiguration()
                                               .Timeout(TimeSpan.FromSeconds(2))
                                               .Take(1).ToTask();

            await client.Config.SetConfig("Test", new RavenJObject());

            var configChange = await notificationTask;

            Assert.Equal("Test", configChange.Name);
            Assert.Equal(ConfigurationChangeAction.Set, configChange.Action);
        }

		[Fact]
		public async Task NotificationsIsReceivedWhenConfigIsDeleted()
        {
            var notificationTask = client.Notifications.ForConfiguration()
                                                .Timeout(TimeSpan.FromSeconds(2))
                                                .Take(1).ToTask();           

            await client.Config.DeleteConfig("Test");

            var configChange = await notificationTask;

            Assert.Equal("Test", configChange.Name);
            Assert.Equal(ConfigurationChangeAction.Delete, configChange.Action);
        }

		public override void Dispose()
		{
            var serverNotifications = client.Notifications as RemoteFileSystemChanges;
			if (serverNotifications != null)
				serverNotifications.DisposeAsync().Wait();
			base.Dispose();
		}
    }
}
