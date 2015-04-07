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
using Raven.Abstractions.Extensions;
using Raven.Client.FileSystem.Extensions;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Connection;

namespace Raven.Tests.FileSystem.Auth
{
    public class ClientOAuthAuthentication : RavenFilesTestWithLogs
    {
        private const string apiKey = "test/ThisIsMySecret";

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            server.SystemDatabase.Documents.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
            {
                Name = "test",
                Secret = "ThisIsMySecret",
                Enabled = true,
                Databases = new List<ResourceAccess>
                {
                    new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true}, // required to create file system
					new ResourceAccess {TenantId = fileSystemName}
                },
            }), new RavenJObject(), null);
        }

        [Fact]
        public void CanCreateFileSystem()
        {
            NewAsyncClient(enableAuthentication: true, apiKey: apiKey);
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

            var client = NewAsyncClient(enableAuthentication: true, apiKey: apiKey);
            await client.UploadAsync("/dir/abc.txt", ms);

            var stream = await client.DownloadAsync("/dir/abc.txt");
            Assert.Equal(expected, StreamToString(stream));

            await client.RenameAsync("/dir/abc.txt", "/dir/cba.txt");

            var searchResults = await client.SearchOnDirectoryAsync("/dir");

            Assert.Equal(1, searchResults.FileCount);

            var metadata = await client.GetMetadataForAsync("/dir/cba.txt");

            Assert.NotNull(metadata);

            var folders = await client.GetDirectoriesAsync();

            Assert.Equal(1, folders.Length);

            var searchFields = await client.GetSearchFieldsAsync();

            Assert.True(searchFields.Length > 0);

            var guid = await client.GetServerIdAsync();

            Assert.NotEqual(Guid.Empty, guid);

            await client.UpdateMetadataAsync("/dir/cba.txt", new RavenJObject() { { "Meta", "Data" } });

            var results = await client.SearchAsync("Meta:Data");

            Assert.Equal(1, results.FileCount);

            var stats = await client.GetStatisticsAsync();

            Assert.Equal(1, stats.FileCount);
        }

        [Fact]
        public async Task AdminClientWorkWithOAuthEnabled()
        {
            var client = (IAsyncFilesCommandsImpl) NewAsyncClient(enableAuthentication: true, apiKey: apiKey);
	        var adminClient = client.Admin;

            await adminClient.CreateFileSystemAsync(new FileSystemDocument
            {
                Id = "Raven/FileSystem/" + "testName",
                Settings =
                 {
                     { Constants.FileSystem.DataDirectory, Path.Combine("~", Path.Combine("FileSystems", "testName"))}
                 }
            }, "testName");

	        var names = await adminClient.GetNamesAsync();

            Assert.Equal(2, names.Length);
            Assert.Contains("AdminClientWorkWithOAuthEnabled", names);

			var stats = await adminClient.GetStatisticsAsync();            
			Assert.Equal(0, stats.Length); // 0 because our fs aren't active

            using (var createdFsClient = new AsyncFilesServerClient(client.ServerUrl, "testName"))
			{
				await createdFsClient.UploadAsync("foo", new MemoryStream(new byte[] { 1 }));
			}

			await adminClient.DeleteFileSystemAsync("testName", true);
        }

        [Fact]
        public async Task ConfigClientCanWorkWithOAuthEnabled()
        {
            var configClient = NewAsyncClient(enableAuthentication: true, apiKey: apiKey).Configuration;

            await configClient.SetKeyAsync("test-conf", new RavenJObject() { { "key", "value" } });

            var config = await configClient.GetKeyAsync<RavenJObject>("test-conf");

            Assert.Equal("value", config["key"]);

            var names = await configClient.GetKeyNamesAsync();

            Assert.Contains("test-conf", names);

            var configSearch = await configClient.SearchAsync("test");

            Assert.Equal(1, configSearch.TotalCount);
        }

        [Fact]
        public async Task StorageClientCanWorkWithOAuthEnabled()
        {
            var storageClient = NewAsyncClient(enableAuthentication: true, apiKey: apiKey).Storage;

            await storageClient.RetryRenamingAsync();

            await storageClient.CleanUpAsync();
        }

        [Fact]
        public async Task ShouldThrowWhenUsedApiKeyDefinitionDoesNotContainFileSystem()
        {
            var client = NewAsyncClient(enableAuthentication: true, apiKey: apiKey);
            var server = GetServer();

            await client.UploadAsync("abc.bin", new RandomStream(3));

            using (var anotherClient = new AsyncFilesServerClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "ShouldThrow_ApiKeyDoesnContainsThisFS", apiKey: apiKey))
            {
                await anotherClient.EnsureFileSystemExistsAsync(); // will pass because by using this api key we have access to <system> database

                ErrorResponseException errorResponse = null;

                try
                {
                    await anotherClient.UploadAsync("def.bin", new RandomStream(1)); // should throw because a file system ShouldThrow_ApiKeyDoesnContainsThisFS isn't added to ApiKeyDefinition
                }
                catch (ErrorResponseException ex)
                {
	                errorResponse = ex;
                }
                
                Assert.NotNull(errorResponse);
                Assert.Equal(HttpStatusCode.Forbidden, errorResponse.StatusCode);
            }
        }
    }
}