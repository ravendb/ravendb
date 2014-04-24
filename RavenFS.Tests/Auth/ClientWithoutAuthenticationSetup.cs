// -----------------------------------------------------------------------
//  <copyright file="ClientWithoutAuthenticationSetup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.RavenFS;
using Xunit;

namespace RavenFS.Tests.Auth
{
    public class ClientWithoutAuthenticationSetup : RavenFsTestBase
    {
        [Fact]
        public async Task WillUseDefaultNetworkCredentialsWhenServerRequiresAuthentication()
        {
            var server = CreateRavenDbServer(Ports[0], fileSystemName: "WillUseDefaultCredentials", enableAuthentication: true); // enable authentication

            using (var client = new RavenFileSystemClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "WillUseDefaultCredentials"))
            {
                await client.Admin.CreateFileSystemAsync(new DatabaseDocument()
                {
                    Id = "Raven/FileSystem/" + client.FileSystemName,
                    Settings =
                    {
                        {"Raven/FileSystem/DataDir", Path.Combine("FileSystems", client.FileSystemName)}
                    }
                });

                await client.UploadAsync("a", new MemoryStream(new byte[] { 1, 2 }));

                var ms = new MemoryStream();
                await client.DownloadAsync("a", ms);

                var array = ms.ToArray();
                Assert.Equal(1, array[0]);
                Assert.Equal(2, array[1]);
            }
        }
    }
}