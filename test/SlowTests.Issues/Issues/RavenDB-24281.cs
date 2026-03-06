using System;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24281(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void EntriesReaderWillNotThrowNreWhenUsingOlderIndex()
    {
        var dbName = Guid.NewGuid().ToString();
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "RavenDB_24281.ravendb-snapshot");
        using (var file = File.Create(fullBackupPath))
        {
            using var stream = typeof(RavenDB_24281).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_24281.RavenDB_24281.ravendb-snapshot");
            Assert.NotNull(stream);
            stream.CopyTo(file);
        }
        using (var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax)))
        using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = dbName }))
        {
            using var session = store.OpenSession(database: dbName);
            var result = session.Query<Dto>().Where(x => x.Name == "ABC" && x.Name2 == 1).ToList();
            Assert.Equal(2, result.Count);
        }
    }

    private record Dto(string Name, int Name2);
}
