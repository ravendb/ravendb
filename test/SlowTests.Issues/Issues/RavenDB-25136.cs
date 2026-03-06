using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25136(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Querying)]
    public async Task CoraxCanSortReduceResultsWithPagingAndTermsReader()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "RavenDB_25136.ravendb-snapshot");
        ExtractFile(fullBackupPath);
        using (var store = GetDocumentStore())
        {
            var databaseName = GetDatabaseName();
            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
            {
                await using (var bulkInsert = store.BulkInsert(database: databaseName))
                {
                    for (int i = 0; i < 65537; i++)
                            await bulkInsert.StoreAsync(new Dto { Name = "_" + i });
                }
                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenAsyncSession(database: databaseName))
                {
                    var results = await session.Query<Index.IndexResult, Index>()
                        .Where(x => x.Name.StartsWith("_"))
                        .OrderByDescending(x => x.Count)
                        .Skip(1)
                        .Take(1)
                        .ToListAsync();
                    
                    Assert.Equal(1, results.Count);
                    Assert.Equal("_1", results[0].Name);
                }
            }
        }

        void ExtractFile(string path)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_25136).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_25136.RavenDB_25136.ravendb-snapshot"))
            {
                stream.CopyTo(file);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Dto, Index.IndexResult>
    {
        public class IndexResult
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public Index()
        {
            Map = dtos => from dto in dtos
                select new IndexResult()
                {
                    Name = dto.Name,
                    Count = 1
                };

            Reduce = results => from result in results
                group result by result.Name
                into g
                select new IndexResult()
                {
                    Name = g.Key,
                    Count = g.Sum(x => x.Count)
                };
        }
    }
}
