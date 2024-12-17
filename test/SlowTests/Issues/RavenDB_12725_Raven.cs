using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12725_Raven : RavenTestBase
    {
        public RavenDB_12725_Raven(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanLoadDatabaseAndIgnoreMissingJournals()
        {
            UseNewLocalServer(new Dictionary<string, string>
            {
                {RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors), "true"}
            });

            var path = NewDataPath();

            DocumentDatabase db;

            using (var store = GetDocumentStore(new Options()
            {
                Path = path
            }))
            {
                db = await GetDatabase(store.Database);
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());
            }

            db.Dispose();

            var journalPath = Path.Combine(path, "Journals");

            var firstJournal = new DirectoryInfo(journalPath).GetFiles("*.journal").OrderBy(x => x.Name).First();

            File.Delete(firstJournal.FullName);

            using (GetDocumentStore(new Options()
            {
                Path = path
            }))
            {
            }
        }
    }
}
