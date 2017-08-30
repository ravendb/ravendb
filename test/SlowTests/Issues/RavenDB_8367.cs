using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8367 : RavenTestBase
    {
        [Fact]
        public async Task RavenExpirationDateShouldBeTranslatedToExpiresBySmuggler()
        {
            const string legacyExpiresKey = "Raven-Expiration-Date";

            var exportPath = NewDataPath(forceCreateDir: true);
            var exportFile = Path.Combine(exportPath, "export.ravendbdump");
            var expires = DateTime.UtcNow.ToString(Default.DateTimeOffsetFormatsToWrite);

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company
                    {
                        Name = "Company1"
                    };

                    session.Store(company, "companies/1");
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[legacyExpiresKey] = expires;

                    session.SaveChanges();
                }

                await store.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), exportFile);
            }

            using (var store = GetDocumentStore())
            {
                await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), exportFile);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal("Company1", company.Name);

                    var metadata = session.Advanced.GetMetadataFor(company);
                    Assert.False(metadata.TryGetValue(legacyExpiresKey, out _));

                    Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Expires, out string e));
                    Assert.Equal(expires, e);
                }
            }
        }
    }
}
