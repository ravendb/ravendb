using System;
using System.IO;
using System.Net.Http;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.Connection;
using Raven.Client.FileSystem;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.FileSystem
{
    public class Notifications : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task EventsShouldWorkWithoutSingleAuthToken()
        {
            var store = NewStore();

            var httpClient = new HttpClient();
            var response = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, store.Url.ForFilesystem(store.DefaultFileSystem) + string.Format("/changes/events?id=bL5rh&coolDownWithDataLoss=5000&isMultyTenantTransport=false")), HttpCompletionOption.ResponseHeadersRead);

            Assert.True(response.IsSuccessStatusCode);
        }

        [Fact]
        public async Task EventsShouldWorkWithSingleAuthToken()
        {
            var store = NewStore();
            var client = (AsyncFilesServerClient)store.AsyncFilesCommands;

            var request = store
                .JsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store.Url.ForFilesystem(store.DefaultFileSystem) + "/singleAuthToken", HttpMethod.Get, client.PrimaryCredentials, store.Conventions));

            var json = await request.ReadResponseJsonAsync();
            var token = json.Value<string>("Token");

            var httpClient = new HttpClient();
            var response = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, store.Url.ForFilesystem(store.DefaultFileSystem) + string.Format("/changes/events?singleUseAuthToken={0}&id=bL5rh&coolDownWithDataLoss=5000&isMultyTenantTransport=false", token)), HttpCompletionOption.ResponseHeadersRead);

            Assert.True(response.IsSuccessStatusCode);
        }

        [Fact]
        public async Task NotificationReceivedWhenFileAdded()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            var changes = store.Changes();
            var notificationTask = changes.ForFolder("/")
                                        .Timeout(TimeSpan.FromSeconds(2))
                                        .Take(1).ToTask();

            changes.WaitForAllPendingSubscriptions();

            await client.UploadAsync("abc.txt", new MemoryStream());

            var fileChange = await notificationTask;

            Assert.Equal("/abc.txt", fileChange.File);
            Assert.Equal(FileChangeAction.Add, fileChange.Action);
        }

        [Fact]
        public async Task NotificationReceivedWhenFileDeleted()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            await client.UploadAsync("abc.txt", new MemoryStream());

            var changes = store.Changes();
            var notificationTask = changes.ForFolder("/")
                                               .Timeout(TimeSpan.FromSeconds(2))
                                               .Take(1).ToTask();

            changes.WaitForAllPendingSubscriptions();

            await client.DeleteAsync("abc.txt");

            var fileChange = await notificationTask;

            Assert.Equal("/abc.txt", fileChange.File);
            Assert.Equal(FileChangeAction.Delete, fileChange.Action);
        }

        [Fact]
        public async Task NotificationReceivedWhenFileUpdated()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            await client.UploadAsync("abc.txt", new MemoryStream());

            var changes = store.Changes();
            var notificationTask = changes.ForFolder("/")
                                                .Timeout(TimeSpan.FromSeconds(2))
                                                .Take(1).ToTask();

            changes.WaitForAllPendingSubscriptions();

            await client.UpdateMetadataAsync("abc.txt", new RavenJObject { { "MyMetadata", "MyValue" } });

            var fileChange = await notificationTask;

            Assert.Equal("/abc.txt", fileChange.File);
            Assert.Equal(FileChangeAction.Update, fileChange.Action);
        }

        [Fact]
        public async Task NotificationsReceivedWhenFileRenamed()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            await client.UploadAsync("abc.txt", new MemoryStream());

            var changes = store.Changes();
            var notificationTask = changes.ForFolder("/")
                                                .Buffer(TimeSpan.FromSeconds(5))
                                                .Take(1).ToTask();

            changes.WaitForAllPendingSubscriptions();

            await client.RenameAsync("abc.txt", "newName.txt");

            var fileChanges = await notificationTask;

            Console.WriteLine("Notification count: " + fileChanges.Count);
            Assert.Equal("/abc.txt", fileChanges[0].File);
            Assert.Equal(FileChangeAction.Renaming, fileChanges[0].Action);
            Assert.Equal("/newName.txt", fileChanges[1].File);
            Assert.Equal(FileChangeAction.Renamed, fileChanges[1].Action);
        }

        [Fact]
        public async Task NotificationsReceivedWhenFileCopied()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            await client.UploadAsync("abc.txt", new MemoryStream());

            var changes = store.Changes();
            var notificationTask = changes.ForFolder("/")
                                                .Buffer(TimeSpan.FromSeconds(5))
                                                .Take(1).ToTask();

            changes.WaitForAllPendingSubscriptions();

            await client.CopyAsync("abc.txt", "newName.txt");

            var fileChanges = await notificationTask;

            Console.WriteLine("Notification count: " + fileChanges.Count);
            Assert.Equal("/newName.txt", fileChanges[0].File);
            Assert.Equal(FileChangeAction.Add, fileChanges[0].Action);
        }


        [Fact]
        public async Task NotificationsAreOnlyReceivedForFilesInGivenFolder()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            var changes = store.Changes();
            var notificationTask = changes.ForFolder("/Folder")
                                                .Buffer(TimeSpan.FromSeconds(2))
                                                .Take(1).ToTask();

            changes.WaitForAllPendingSubscriptions();

            await client.UploadAsync("AnotherFolder/abc.txt", new MemoryStream());

            var notifications = await notificationTask;

            Assert.Equal(0, notifications.Count);
        }

        [Fact]
        public async Task NotificationsIsReceivedWhenConfigIsUpdated()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            var changesApi = store.Changes();
            await changesApi.Task; // BARRIER: Ensures we are already connected to avoid a race condition and fail to get the notification.

            var notificationTask = changesApi.ForConfiguration()
                                        .Timeout(TimeSpan.FromSeconds(5))
                                        .Take(1).ToTask();

            await client.Configuration.SetKeyAsync("Test", new RavenJObject());

            var configChange = await notificationTask;

            Assert.Equal("Test", configChange.Name);
            Assert.Equal(ConfigurationChangeAction.Set, configChange.Action);
        }

        [Fact]
        public async Task NotificationsIsReceivedWhenConfigIsDeleted()
        {
            var store = NewStore();
            var client = store.AsyncFilesCommands;

            var changesApi = store.Changes();
            await changesApi.Task; // BARRIER: Ensures we are already connected to avoid a race condition and fail to get the notification.

            var notificationTask = changesApi.ForConfiguration()
                                        .Timeout(TimeSpan.FromSeconds(2))
                                        .Take(1).ToTask();

            await client.Configuration.DeleteKeyAsync("Test");

            var configChange = await notificationTask;

            Assert.Equal("Test", configChange.Name);
            Assert.Equal(ConfigurationChangeAction.Delete, configChange.Action);
        }
    }
}
