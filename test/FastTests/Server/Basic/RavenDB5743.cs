using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Basic
{
    public class RavenDB5743 : RavenTestBase
    {
        [Fact]
        public async Task WillFilterMetadataPropertiesStartingWithAt()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "Company Name" };
                    await session.StoreAsync(company, "users/1");
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata["@foo"] = "bar";
                    metadata["custom-info"] = "should be there";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(company3);
                    Assert.False(metadata.ContainsKey("@foo"));
                    Assert.Equal("should be there", metadata.GetString("custom-info"));
                }
            }
        }
    }
}
