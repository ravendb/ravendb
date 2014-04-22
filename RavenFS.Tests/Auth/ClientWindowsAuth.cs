// -----------------------------------------------------------------------
//  <copyright file="ClientWindowsAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using RavenFS.Tests.Synchronization.IO;
using Xunit;

namespace RavenFS.Tests.Auth
{
    public class ClientWindowsAuth : RavenFsTestBase
    {
        private string username = "local_user_test";

        private string password = "local_user_test";

        private string domain = "local_machine_name_test";

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            server.SystemDatabase.Documents.Put("Raven/Authorization/WindowsSettings", null,
                                      RavenJObject.FromObject(new WindowsAuthDocument
                                      {
                                          RequiredUsers = new List<WindowsAuthData>
                                          {
                                              new WindowsAuthData()
                                              {
                                                  Name = string.Format("{0}\\{1}", domain, username),
                                                  Enabled = true,
                                                  Databases = new List<DatabaseAccess>
                                                  {
                                                      new DatabaseAccess {TenantId = Constants.SystemDatabase, Admin = true} // required to create file system
                                                  },
                                                  FileSystems = new List<FileSystemAccess>()
                                                  {
                                                      new FileSystemAccess() {TenantId = fileSystemName}
                                                  }
                                              }
                                          }
                                      }), new RavenJObject(), null);
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task CanWorkWithWinAuthEnabled()
        {
            var client = NewClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain));

            var ms = new MemoryStream(new byte[]{1, 2, 4});

            await client.UploadAsync("/dir/ms.bin", ms);

            var result = new MemoryStream();

            await client.DownloadAsync("/dir/ms.bin", result);

            ms.Position = 0;
            result.Position = 0;

            Assert.Equal(ms.GetMD5Hash(), result.GetMD5Hash());
            await client.RenameAsync("/dir/ms.bin", "/dir/sm.bin");

            var searchResults = await client.GetFilesAsync("/dir");

            Assert.Equal(1, searchResults.FileCount);

            var metadata = await client.GetMetadataForAsync("/dir/sm.bin");

            Assert.NotNull(metadata);

            var folders = await client.GetFoldersAsync();

            Assert.Equal(1, folders.Length);

            var searchFields = await client.GetSearchFieldsAsync();

            Assert.True(searchFields.Length > 0);

            var guid = await client.GetServerId();

            Assert.NotEqual(Guid.Empty, guid);

            await client.UpdateMetadataAsync("/dir/sm.bin", new RavenJObject() { { "Meta", "Data" } });

            var results = await client.SearchAsync("Meta:Data");

            Assert.Equal(1, results.FileCount);

            var stats = await client.StatsAsync();

            Assert.Equal(1, stats.FileCount);
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task AdminClientWorkWithWinAuthEnabled()
        {
            var adminClient = NewClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain)).Admin;

            await adminClient.CreateFileSystemAsync(new DatabaseDocument
            {
                Id = "Raven/FileSystem/" + "testName",
                Settings =
                 {
                     {"Raven/FileSystem/DataDir", Path.Combine("~", Path.Combine("FileSystems", "testName"))}
                 }
            }, "testName");

            var names = await adminClient.GetFileSystemsNames();

            Assert.Contains("AdminClientWorkWithWinAuthEnabled", names);

            var stats = await adminClient.GetFileSystemsStats();

            Assert.NotNull(stats.FirstOrDefault(x => x.Name == "AdminClientWorkWithWinAuthEnabled"));
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task ConfigClientCanWorkWithWinAuthEnabled()
        {
            var configClient = NewClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain)).Config;

            await configClient.SetConfig("test-conf", new NameValueCollection() { { "key", "value" } });

            var config = await configClient.GetConfig<NameValueCollection>("test-conf");

            Assert.Equal("value", config["key"]);

            var names = await configClient.GetConfigNames();

            Assert.Contains("test-conf", names);

            var configSearch = await configClient.SearchAsync("test");

            Assert.Equal(1, configSearch.TotalCount);

            await configClient.SetDestinationsConfig(new SynchronizationDestination() { ServerUrl = "http://local:123", FileSystem = "test" });
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task StorageClientCanWorkWithWinAuthEnabled()
        {
            var storageClient = NewClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain)).Storage;

            await storageClient.RetryRenaming();

            await storageClient.CleanUp();
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task ShouldThrowWhenWindowsDocumentDoesNotContainFileSystem()
        {
            // in this test be careful if the specified credentials belong to admin user or not

            var client = NewClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain));
            var server = GetServer();

            await client.UploadAsync("abc.bin", new RandomStream(3));

            using (var anotherClient = new RavenFileSystemClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "ShouldThrow_WindowsDocumentDoesnContainsThisFS", 
                credentials: new NetworkCredential(username, password, domain)))
            {
                await anotherClient.EnsureFileSystemExistsAsync(); // will pass because by using this api key we have access to <system> database

                ErrorResponseException errorResponse = null;

                try
                {
                    await anotherClient.UploadAsync("def.bin", new RandomStream(1)); // should throw because a file system ShouldThrow_ApiKeyDoesnContainsThisFS isn't added to ApiKeyDefinition
                }
                catch (InvalidOperationException ex)
                {
                    errorResponse = ex.InnerException as ErrorResponseException;
                }

                Assert.NotNull(errorResponse);
                Assert.Equal(HttpStatusCode.Forbidden, errorResponse.StatusCode);
            }
        }
    }
}