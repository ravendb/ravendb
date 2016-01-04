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
using Raven.Database.Extensions;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.FileSystem.Synchronization.IO;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Helpers.Util;

using Xunit;

namespace Raven.Tests.FileSystem.Auth
{
    public class ClientWindowsAuth : RavenFilesTestWithLogs
    {
        protected override void ModifyStore(FilesStore store)
        {
            FactIfWindowsAuthenticationIsAvailable.LoadCredentials();
            ConfigurationHelper.ApplySettingsToConventions(store.Conventions);

            base.ModifyStore(store);
        }

        public ClientWindowsAuth()
        {
            FactIfWindowsAuthenticationIsAvailable.LoadCredentials();
        }

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            server.SystemDatabase.Documents.Put("Raven/Authorization/WindowsSettings", null,
                                      RavenJObject.FromObject(new WindowsAuthDocument
                                      {
                                          RequiredUsers = new List<WindowsAuthData>
                                          {
                                              new WindowsAuthData()
                                              {
                                                  Name = string.Format("{0}\\{1}", FactIfWindowsAuthenticationIsAvailable.Admin.Domain, FactIfWindowsAuthenticationIsAvailable.Admin.UserName),
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

        [Fact]
        public async Task CanWorkWithWinAuthEnabled()
        {
            var client = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Admin.UserName, FactIfWindowsAuthenticationIsAvailable.Admin.Password, FactIfWindowsAuthenticationIsAvailable.Admin.Domain));

            var ms = new MemoryStream(new byte[1024 * 1024 * 10]);

            await client.UploadAsync("/dir/ms.bin", ms);
            ms.Position = 0;
            await client.UploadAsync("/dir/ms.bin", ms);

            var result = new MemoryStream();
            (await client.DownloadAsync("/dir/ms.bin")).CopyTo(result);

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

        [Fact]
        public async Task AdminClientWorkWithWinAuthEnabled()
        {
            var client = (IAsyncFilesCommandsImpl)NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Admin.UserName, FactIfWindowsAuthenticationIsAvailable.Admin.Password, FactIfWindowsAuthenticationIsAvailable.Admin.Domain));
            var adminClient = client.Admin;

            await adminClient.CreateFileSystemAsync(MultiDatabase.CreateFileSystemDocument("testName"), "testName");

            using (var createdFsClient = new AsyncFilesServerClient(client.ServerUrl, "testName", conventions: client.Conventions, credentials: new OperationCredentials(null, new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Admin.UserName, FactIfWindowsAuthenticationIsAvailable.Admin.Password, FactIfWindowsAuthenticationIsAvailable.Admin.Domain))))
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

        [Fact]
        public async Task ConfigClientCanWorkWithWinAuthEnabled()
        {
            var configClient = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Admin.UserName, FactIfWindowsAuthenticationIsAvailable.Admin.Password, FactIfWindowsAuthenticationIsAvailable.Admin.Domain)).Configuration;

            await configClient.SetKeyAsync("test-conf", new RavenJObject() { { "key", "value" } });

            var config = await configClient.GetKeyAsync<RavenJObject>("test-conf");

            Assert.Equal("value", config["key"]);

            var names = await configClient.GetKeyNamesAsync();

            Assert.Contains("test-conf", names);

            var configSearch = await configClient.SearchAsync("test");

            Assert.Equal(1, configSearch.TotalCount);            
        }

        [Fact]
        public async Task StorageClientCanWorkWithWinAuthEnabled()
        {
            var storageClient = NewAsyncClient(enableAuthentication: true, credentials: new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Admin.UserName, FactIfWindowsAuthenticationIsAvailable.Admin.Password, FactIfWindowsAuthenticationIsAvailable.Admin.Domain)).Storage;

            await storageClient.RetryRenamingAsync();

            await storageClient.CleanUpAsync();
        }
    }
}
