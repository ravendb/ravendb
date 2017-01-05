using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using FastTests.Server.Documents.Versioning;
using Xunit;

namespace FastTests.Server.Basic
{
    public class RavenDB5743 : RavenTestBase
    {
        [Fact]
        public async Task WillFilterMetadataPropertiesStartingWithAt()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    session.Advanced.GetMetadataFor(company)["@foo"] = "bar";
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    Assert.Null(session.Advanced.GetMetadataFor(company3).Value<string>("@foo"));
                }
            }
        }
    }
}