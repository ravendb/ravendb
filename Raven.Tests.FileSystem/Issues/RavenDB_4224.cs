// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4224.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4224 : RavenFilesTestBase
    {
        [Fact]
        public async Task can_stream_file_headers_by_using_session()
        {
            using (var store = this.NewStore())
            {
                store.Conventions.MaxNumberOfRequestsPerSession = 100;

                Etag fromEtag = Etag.Empty;

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 50; i++)
                        session.RegisterUpload(i + ".file", CreateUniformFileStream(10));

                    await session.SaveChangesAsync();

                    var tenthFile = await session.LoadFileAsync("9.file");
                    fromEtag = tenthFile.Etag;
                }

                int count = 0;
                using (var session = store.OpenAsyncSession())
                {
                    using (var reader = await session.Advanced.StreamFileHeadersAsync(fromEtag: fromEtag, pageSize: 10))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<FileHeader>(reader.Current);
                        }
                    }
                }

                Assert.Equal(10, count);
            }
        }
    }
}