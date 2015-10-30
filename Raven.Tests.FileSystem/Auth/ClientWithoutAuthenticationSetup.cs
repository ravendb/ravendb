// -----------------------------------------------------------------------
//  <copyright file="ClientWithoutAuthenticationSetup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Xunit;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;

namespace Raven.Tests.FileSystem.Auth
{
    public class ClientWithoutAuthenticationSetup : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task WillUseDefaultNetworkCredentialsWhenServerRequiresAuthentication_CommandsUsage()
        {
            var server = CreateServer(8079, fileSystemName: "WillUseDefaultCredentials", enableAuthentication: true); // enable authentication

            using (var client = new AsyncFilesServerClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "WillUseDefaultCredentials"))
            {
                await client.Admin.CreateFileSystemAsync(new FileSystemDocument()
                {
                    Id = "Raven/FileSystem/" + client.FileSystemName,
                    Settings =
                    {
                        {Constants.FileSystem.DataDirectory, Path.Combine("FileSystems", client.FileSystemName)}
                    }
                });

                await client.UploadAsync("a", new MemoryStream(new byte[] { 1, 2 }));
                await client.UploadAsync("b", new MemoryStream(new byte[] { 2, 1, 0 }));

                var ms = new MemoryStream();
                (await client.DownloadAsync("a")).CopyTo(ms);

                var array = ms.ToArray();
                Assert.Equal(1, array[0]);
                Assert.Equal(2, array[1]);

                ms = new MemoryStream();
                (await client.DownloadAsync("b")).CopyTo(ms);

                array = ms.ToArray();
                Assert.Equal(2, array[0]);
                Assert.Equal(1, array[1]);
                Assert.Equal(0, array[2]);
            }
        }

        [Fact]
        public async Task WillUseDefaultNetworkCredentialsWhenServerRequiresAuthentication_SessionUsage()
        {
            using(var store = NewStore(enableAuthentication: true)) // enable authentication
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("a", new MemoryStream(new byte[] { 1, 2 }));
                    session.RegisterUpload("b", new MemoryStream(new byte[] { 2, 1, 0 }));

                    await session.SaveChangesAsync();
                }

                var ms = new MemoryStream();
                (await store.AsyncFilesCommands.DownloadAsync("a")).CopyTo(ms);

                var array = ms.ToArray();
                Assert.Equal(1, array[0]);
                Assert.Equal(2, array[1]);

                ms = new MemoryStream();
                (await store.AsyncFilesCommands.DownloadAsync("b")).CopyTo(ms);

                array = ms.ToArray();
                Assert.Equal(2, array[0]);
                Assert.Equal(1, array[1]);
                Assert.Equal(0, array[2]);
            }
        }

        [Fact]
        public async Task WillUseDefaultNetworkCredentialsWhenServerRequiresAuthentication_SessionUsage_DefferedAction()
        {
            using (var store = NewStore(enableAuthentication: true)) // enable authentication
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("a", 2, s =>
                    {
                        s.WriteByte(1);
                        s.WriteByte(2);
                    });
                    session.RegisterUpload("b", 3, s =>
                    {
                        s.WriteByte(2);
                        s.WriteByte(1);
                        s.WriteByte(0);
                    });

                    await session.SaveChangesAsync();
                }

                var ms = new MemoryStream();
                (await store.AsyncFilesCommands.DownloadAsync("a")).CopyTo(ms);

                var array = ms.ToArray();
                Assert.Equal(1, array[0]);
                Assert.Equal(2, array[1]);

                ms = new MemoryStream();
                (await store.AsyncFilesCommands.DownloadAsync("b")).CopyTo(ms);

                array = ms.ToArray();
                Assert.Equal(2, array[0]);
                Assert.Equal(1, array[1]);
                Assert.Equal(0, array[2]);
            }
        }
    }
}
