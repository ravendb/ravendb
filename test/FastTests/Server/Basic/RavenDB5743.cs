using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using FastTests.Server.Documents.Versioning;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Basic
{
    public class RavenDB5743 : RavenTestBase
    {
        [Fact(Skip = "RavenDB-6193")]
        public async Task WillFilterMetadataPropertiesStartingWithAt()
        {
            var company = new Company { Name = "Company Name" };
            using (var store = GetDocumentStore())
            {
                await VersioningHelper.SetupVersioning(Server.ServerStore, store.DefaultDatabase);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    session.Advanced.GetMetadataFor(company)["@foo"] = "bar";
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.False(metadata.ContainsKey("@foo"));
                }
            }
        }
    }
}