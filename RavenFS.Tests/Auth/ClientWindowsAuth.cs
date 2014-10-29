// -----------------------------------------------------------------------
//  <copyright file="ClientWindowsAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using RavenFS.Tests.Synchronization.IO;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
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
                                                  Databases = new List<ResourceAccess>
                                                  {
                                                      new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true}, // required to create file system
													  new ResourceAccess {TenantId = fileSystemName}
                                                  }
                                              }
                                          }
                                      }), new RavenJObject(), null);
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task CanWorkWithWinAuthEnabled()
        {
            var client = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain));

            var ms = new MemoryStream(new byte[]{1, 2, 4});

            await client.UploadAsync("/dir/ms.bin", ms);

            var result = await client.DownloadAsync("/dir/ms.bin");

            ms.Position = 0;
            result.Position = 0;

            Assert.Equal(ms.GetMD5Hash(), result.GetMD5Hash());
            await client.RenameAsync("/dir/ms.bin", "/dir/sm.bin");

            var searchResults = await client.SearchOnDirectoryAsync("/dir");

            Assert.Equal(1, searchResults.FileCount);

            var metadata = await client.GetMetadataForAsync("/dir/sm.bin");

            Assert.NotNull(metadata);

            var folders = await client.GetDirectoriesAsync();

            Assert.Equal(1, folders.Length);

            var searchFields = await client.GetSearchFieldsAsync();

            Assert.True(searchFields.Length > 0);

            var guid = await client.GetServerIdAsync();

            Assert.NotEqual(Guid.Empty, guid);

            await client.UpdateMetadataAsync("/dir/sm.bin", new RavenJObject() { { "Meta", "Data" } });

            var results = await client.SearchAsync("Meta:Data");

            Assert.Equal(1, results.FileCount);

            var stats = await client.GetStatisticsAsync();

            Assert.Equal(1, stats.FileCount);
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task AdminClientWorkWithWinAuthEnabled()
        {
            var client = (IAsyncFilesCommandsImpl)NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain));
	        var adminClient = client.Admin;

            await adminClient.CreateFileSystemAsync(new FileSystemDocument
            {
                Id = "Raven/FileSystem/" + "testName",
                Settings =
                 {
                     {"Raven/FileSystem/DataDir", Path.Combine("~", Path.Combine("FileSystems", "testName"))}
                 }
            }, "testName");

            using (var createdFsClient = new AsyncFilesServerClient(client.ServerUrl, "testName", new NetworkCredential(username, password, domain)))
	        {
		        await createdFsClient.UploadAsync("foo", new MemoryStream(new byte[] {1}));
	        }

            var names = await adminClient.GetNamesAsync();

            Assert.Contains("testName", names);

            var stats = await adminClient.GetStatisticsAsync();

			Assert.NotNull(stats.FirstOrDefault(x => x.Name == "testName"));

	        await adminClient.DeleteFileSystemAsync("testName");

			names = await adminClient.GetNamesAsync();

			Assert.DoesNotContain("testName", names);
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task ConfigClientCanWorkWithWinAuthEnabled()
        {
            var configClient = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain)).Configuration;

            await configClient.SetKeyAsync("test-conf", new NameValueCollection() { { "key", "value" } });

            var config = await configClient.GetKeyAsync<NameValueCollection>("test-conf");

            Assert.Equal("value", config["key"]);

            var names = await configClient.GetKeyNamesAsync();

            Assert.Contains("test-conf", names);

            var configSearch = await configClient.SearchAsync("test");

            Assert.Equal(1, configSearch.TotalCount);            
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task StorageClientCanWorkWithWinAuthEnabled()
        {
            var storageClient = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain)).Storage;

            await storageClient.RetryRenamingAsync();

            await storageClient.CleanUpAsync();
        }

        [Fact(Skip = "This test rely on actual Windows Account name/password.")]
        public async Task ShouldThrowWhenWindowsDocumentDoesNotContainFileSystem()
        {
            // in this test be careful if the specified credentials belong to admin user or not

            var client = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(username, password, domain));
            var server = GetServer();

            await client.UploadAsync("abc.bin", new RandomStream(3));

            using (var anotherClient = new AsyncFilesServerClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "ShouldThrow_WindowsDocumentDoesnContainsThisFS", 
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