// -----------------------------------------------------------------------
//  <copyright file="ClientOAuthAuthentication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace RavenFS.Tests.Auth
{
    public class ClientOAuthAuthentication : RavenFsTestBase
    {
        private const string apiKey = "test/ThisIsMySecret";

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            server.SystemDatabase.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
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
                    new FileSystemAccess(){ TenantId = fileSystemName }
                }
                
            }), new RavenJObject(), null);
        }

        [Fact]
        public void CanCreateFileSystem()
        {
            NewClient(enableAuthentication: true, apiKey: apiKey);
        }

        [Fact]
        public async Task CanUploadAndDownload()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            var client = NewClient(enableAuthentication: true, apiKey: apiKey);
            await client.UploadAsync("abc.txt", ms);

            var stream = new MemoryStream();

            await client.DownloadAsync("abc.txt", stream);
            Assert.Equal(expected, StreamToString(stream));
        }
    }
}