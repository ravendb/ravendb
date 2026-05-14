using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8367 : RavenTestBase
    {
        public RavenDB_8367(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.ExpirationRefresh)]
        public async Task RavenExpirationDateShouldBeTranslatedToExpiresBySmuggler()
        {
            const string legacyExpiresKey = "Raven-Expiration-Date";

            using (var store = GetDocumentStore())
            {
                using (var stream = GetType().Assembly.GetManifestResourceStream("SlowTests.Smuggler.Data.Legacy-Raven-Expiration-Date.ravendbdump"))
                {
                    Assert.NotNull(stream);

                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                    operation.WaitForCompletion<SmugglerResult>(TimeSpan.FromSeconds(30));
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Equal("Grisha", user.Name);

                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.False(metadata.TryGetValue(legacyExpiresKey, out _));

                    Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Expires, out string e));
                    Assert.Equal("2060-05-06T00:00:00.0000000", e);
                }
            }
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.ExpirationRefresh)]
        public async Task RavenExpirationDateShouldNotBeTranslatedToExpiresBySmuggler()
        {
            const string legacyExpiresKey = "Raven-Expiration-Date";

            var exportPath = NewDataPath(forceCreateDir: true);
            var exportFile = Path.Combine(exportPath, "export.ravendbdump");
            var expires = DateTime.UtcNow.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

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

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var store = GetDocumentStore())
            {
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal("Company1", company.Name);

                    var metadata = session.Advanced.GetMetadataFor(company);
                    Assert.True(metadata.TryGetValue(legacyExpiresKey, out string e));
                    Assert.Equal(expires, e);

                    Assert.False(metadata.TryGetValue(Constants.Documents.Metadata.Expires, out _));
                }
            }
        }
    }
}
