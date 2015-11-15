// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3692.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_3692 : RavenFilesTestBase
    {
        [Fact]
        public async Task upload_download_rename_and_delete_file_with_hash_character_in_name()
        {
            using (var filesStore = NewStore())
            {
                using (var session = filesStore.OpenAsyncSession())
                {
                    session.RegisterUpload("test/#test.txt", new MemoryStream(new byte[] { 1 }));
                    await session.SaveChangesAsync();
                }

                using (var session = filesStore.OpenAsyncSession())
                {
                    Assert.NotNull(await session.LoadFileAsync("test/#test.txt"));

                    using (var stream = await session.DownloadAsync("test/#test.txt"))
                    {
                        var data = await stream.ReadDataAsync();

                        Assert.Equal(1, data.Length);
                    }

                    session.RegisterRename("test/#test.txt", "test/#renamed.txt");

                    await session.SaveChangesAsync();

                    Assert.NotNull(await session.LoadFileAsync("test/#renamed.txt"));
                    Assert.Null(await session.LoadFileAsync("test/#test.txt"));

                    session.RegisterFileDeletion("test/#renamed.txt");

                    await session.SaveChangesAsync();

                    Assert.Null(await session.LoadFileAsync("test/#renamed.txt"));
                }
            }
        }
    }
}
