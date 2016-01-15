using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.FileSystem
{
    public class StreamingTests : RavenFilesTestWithLogs
    {

        [Fact]
        public async Task CanStreamFilesFromSpecifiedEtag()
        {
            using (var store = this.NewStore())
            {
                store.Conventions.MaxNumberOfRequestsPerSession = 100;

                Etag fromEtag;

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
                    using (var reader = await session.Commands.StreamFileHeadersAsync(fromEtag: fromEtag))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<FileHeader>(reader.Current);
                        }
                    }
                }
                Assert.Equal(40, count);
            }
        }

        [Fact]
        public async Task CanStreamFilesFromSpecifiedEtagWithPages()
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
                    using (var reader = await session.Commands.StreamFileHeadersAsync(fromEtag: fromEtag, pageSize: 10))
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
