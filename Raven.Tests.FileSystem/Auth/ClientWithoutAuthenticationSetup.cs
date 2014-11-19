// -----------------------------------------------------------------------
//  <copyright file="ClientWithoutAuthenticationSetup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Helpers;
using Xunit;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;

namespace Raven.Tests.FileSystem.Auth
{
    public class ClientWithoutAuthenticationSetup : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task WillUseDefaultNetworkCredentialsWhenServerRequiresAuthentication()
        {
            var server = CreateServer(Ports[0], fileSystemName: "WillUseDefaultCredentials", enableAuthentication: true); // enable authentication

            using (var client = new AsyncFilesServerClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "WillUseDefaultCredentials"))
            {
                await client.Admin.CreateFileSystemAsync(new FileSystemDocument()
                {
                    Id = "Raven/FileSystem/" + client.FileSystem,
                    Settings =
                    {
                        {"Raven/FileSystem/DataDir", Path.Combine("FileSystems", client.FileSystem)}
                    }
                });

                await client.UploadAsync("a", new MemoryStream(new byte[] { 1, 2 }));

                var ms = new MemoryStream();
                (await client.DownloadAsync("a")).CopyTo(ms);

                var array = ms.ToArray();
                Assert.Equal(1, array[0]);
                Assert.Equal(2, array[1]);
            }
        }
    }
}