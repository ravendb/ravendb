// -----------------------------------------------------------------------
//  <copyright file="RavenDB_5347.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_5347 : RavenFilesTestWithLogs
    {
        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public void load_non_existing_file(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                Assert.DoesNotThrow(() =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var headers = session.LoadFileAsync(new[] { "filekey/that/is/not/exist" }).Result;
                        Assert.Equal(1, headers.Length);
                        Assert.Null(headers[0]);
                    }
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task load_non_existing_file_multiple_files(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }

                Assert.DoesNotThrow(() =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var files = session.LoadFileAsync(new[] { "/b/test1.file", "test1.file" }).Result;
                        Assert.Equal(2, files.Length);
                        Assert.Null(files[0]);
                        Assert.NotNull(files[1]);
                    }
                });

                Assert.DoesNotThrow(() =>
                {
                    using (var session = store.OpenAsyncSession())
                    {

                        var files = session.LoadFileAsync(new[] { "test1.file", "/b/test1.file" }).Result;
                        Assert.Equal(2, files.Length);
                        Assert.Null(files[1]);
                        Assert.NotNull(files[0]);
                    }
                });
            }
        }
    }
}
