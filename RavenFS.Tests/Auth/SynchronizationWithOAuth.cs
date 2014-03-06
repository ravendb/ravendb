// -----------------------------------------------------------------------
//  <copyright file="SynchronizationWithOAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace RavenFS.Tests.Auth
{
    public class SynchronizationWithOAuth : RavenFsTestBase
    {
        private const string apiKey = "test/ThisIsMySecret";

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            if (server.SystemDatabase.Configuration.Port == Ports[1]) // setup only for destination
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
                        new FileSystemAccess() {TenantId = fileSystemName}
                    }

                }), new RavenJObject(), null);
            }
        }

        [Fact]
        public async Task CanSynchronizeWithOAuth()
        {
            var source = NewClient(0);
            var destination = NewClient(1, enableAuthentication: true, apiKey: apiKey);

            var ms = new MemoryStream(new byte[] {3, 2, 1});

            await source.UploadAsync("ms.bin", new NameValueCollection(), ms);

            var result = await source.Synchronization.StartAsync("ms.bin", destination);

            Assert.Null(result.Exception);
            Assert.Equal(3, result.BytesTransfered);
        }
    }
}