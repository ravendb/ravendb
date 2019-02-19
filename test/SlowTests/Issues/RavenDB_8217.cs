using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Voron.Global;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8217 : RavenTestBase
    {
        [NonLinuxFact]
        public async Task CanGetProperErrorForIndexCreationFailure()
        {
            using (var store = GetDocumentStore(new Options
            {
                Path = NewDataPath()
            }))
            {
                var db = await GetDatabase(store.Database);

                var indexPath = Path.Combine(db.Configuration.Core.DataDirectory.FullPath, "Indexes", "abc");
                Directory.CreateDirectory(indexPath);
                using (File.Create(Path.Combine(indexPath, Constants.DatabaseFilename)))
                {
                    var e = await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "abc",
                        Maps =
                        {
                            "from u in docs select new { u.Name}"
                        }
                    })));
                    Assert.Contains(Constants.DatabaseFilename, e.ToString()); // we want to ensure that the failure to create the db is here.
                }
            }
        }
    }
}
