// -----------------------------------------------------------------------
//  <copyright file="ClientOAuthAuthentication.cs" company="Hibernating Rhinos LTD">
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
using Raven.Json.Linq;
using Raven.Server;
using RavenFS.Tests.Synchronization.IO;
using Xunit;

namespace RavenFS.Tests.Auth
{
    public class ClientOAuthAuthentication : RavenFsTestBase
    {
        private const string apiKey = "test/ThisIsMySecret";

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            server.SystemDatabase.Documents.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
            {
                Name = "test",
                Secret = "ThisIsMySecret",
                Enabled = true,
                Databases = new List<DatabaseAccess>
                {
                    new DatabaseAccess {TenantId = Constants.SystemDatabase, Admin = true}, // required to create file system
                },
                FileSystems = new List<FileSystemAccess>()
                {
                    new FileSystemAccess() {TenantId = fileSystemName}
                }

            }), new RavenJObject(), null);
        }

        [Fact]
        public void CanCreateFileSystem()
        {
            NewClient(enableAuthentication: true, apiKey: apiKey);
        }

        [Fact]
        public async Task CanWorkWithOAuthEnabled()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            var client = NewClient(enableAuthentication: true, apiKey: apiKey);
            await client.UploadAsync("/dir/abc.txt", ms);

            var stream = new MemoryStream();

            await client.DownloadAsync("/dir/abc.txt", stream);
            Assert.Equal(expected, StreamToString(stream));

            await client.RenameAsync("/dir/abc.txt", "/dir/cba.txt");

            var searchResults = await client.GetFilesAsync("/dir");

            Assert.Equal(1, searchResults.FileCount);

            var metadata = await client.GetMetadataForAsync("/dir/cba.txt");

            Assert.NotNull(metadata);

            var folders = await client.GetFoldersAsync();

            Assert.Equal(1, folders.Length);

            var searchFields = await client.GetSearchFieldsAsync();

            Assert.True(searchFields.Length > 0);

            var guid = await client.GetServerId();

            Assert.NotEqual(Guid.Empty, guid);

            await client.UpdateMetadataAsync("/dir/cba.txt", new NameValueCollection() {{"Meta", "Data"}});

            var results = await client.SearchAsync("Meta:Data");

            Assert.Equal(1, results.FileCount);

            var stats = await client.StatsAsync();

            Assert.Equal(1, stats.FileCount);
        }

        [Fact]
        public async Task AdminClientWorkWithOAuthEnabled()
        {
            var adminClient = NewClient(enableAuthentication: true, apiKey: apiKey).Admin;

            await adminClient.CreateFileSystemAsync(new DatabaseDocument
            {
                Id = "Raven/FileSystem/" + "testName",
                Settings =
                 {
                     {"Raven/FileSystem/DataDir", Path.Combine("~", Path.Combine("FileSystems", "testName"))}
                 }
            }, "testName");

            var names = await adminClient.GetFileSystemsNames();

            Assert.Equal(1, names.Length); // will not return 'testName' file system name because used apiKey doesn't have access to a such file system
            Assert.Equal("AdminClientWorkWithOAuthEnabled", names[0]);

            var stats = await adminClient.GetFileSystemsStats();

            Assert.Equal(0, stats.Count); // 0 because our fs aren't active
        }

        [Fact]
        public async Task ConfigClientCanWorkWithOAuthEnabled()
        {
            var configClient = NewClient(enableAuthentication: true, apiKey: apiKey).Config;

            await configClient.SetConfig("test-conf", new NameValueCollection() {{"key", "value"}});

            var config = await configClient.GetConfig("test-conf");

            Assert.Equal("value", config["key"]);

            var names = await configClient.GetConfigNames();

            Assert.Contains("test-conf", names);

            var configSearch = await configClient.SearchAsync("test");

            Assert.Equal(1, configSearch.TotalCount);

            await configClient.SetDestinationsConfig(new SynchronizationDestination(){ServerUrl = "http://local:123", FileSystem = "test"});
        }

        [Fact]
        public async Task StorageClientCanWorkWithOAuthEnabled()
        {
            var storageClient = NewClient(enableAuthentication: true, apiKey: apiKey).Storage;

            await storageClient.RetryRenaming();

            await storageClient.CleanUp();
        }

        [Fact]
        public async Task ShouldThrowWhenUsedApiKeyDefinitionDoesNotContainFileSystem()
        {
            var client = NewClient(enableAuthentication: true, apiKey: apiKey);
            var server = GetServer();

            await client.UploadAsync("abc.bin", new RandomStream(3));

            using (var anotherClient = new RavenFileSystemClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "ShouldThrow_ApiKeyDoesnContainsThisFS", apiKey: apiKey))
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