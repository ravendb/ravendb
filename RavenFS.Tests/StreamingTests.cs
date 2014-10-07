using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Tests.Common.Dto;
using Xunit;

namespace RavenFS.Tests
{
    public class StreamingTests : RavenFilesTestWithLogs
    {

        [Fact]
        public async Task CanStreamFilesFromSpecifiedEtag()
        {
            using (var store = this.NewStore())
            {
                store.Conventions.MaxNumberOfRequestsPerSession = 30;

                Etag fromEtag;

                using (var session = store.OpenAsyncSession())
                {                    
                    for (int i = 0; i < 20; i++)
                        session.RegisterUpload( i + ".file", CreateUniformFileStream(10));

                    await session.SaveChangesAsync();

                    var tenthFile = await session.LoadFileAsync("9.file");
                    fromEtag = tenthFile.Etag;
                }

                int count = 0;
                using (var session = store.OpenAsyncSession())
                {
                    using ( var reader = await session.Commands.StreamFilesAsync(fromEtag: fromEtag))
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
