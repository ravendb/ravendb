using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Bugs
{
    public class Can_query_by_creation_date : RavenFilesTestBase
    {
        [Fact]
        public async Task Can_read_Metadata_async()
        {
            using (var store = NewStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test.txt", new MemoryStream());
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var byLastModified = await session.Query().WhereGreaterThan(x => x.LastModified, DateTime.Now.AddDays(-1)).ToListAsync();
                    var byCreationDate = await session.Query().WhereGreaterThan(x => x.CreationDate, DateTime.Now.AddDays(-1)).ToListAsync();

                    Assert.Equal(1, byLastModified.Count);
                    Assert.Equal(1, byCreationDate.Count);
                }
            }
        }
    }
}