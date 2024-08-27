using System;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22534 : RavenTestBase
{
    public RavenDB_22534(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void BackwardCompatibilityComplexFields()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var file = Path.Combine(backupPath, "RavenDB_22534.ravendb-snapshot");
        ExtractFile(file);

        var restoredDb = GetDatabaseName() + "_restored";
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = restoredDb });

        using (var session = store.OpenSession(restoredDb))
        {
            session.Advanced.WaitForIndexesAfterSaveChanges();
            var index = new Index();
            session.Store(new Dto() { Id = "doc3", DtoInner = new DtoInner() { Name = "3inner" } });
            session.SaveChanges();
            Indexes.WaitForIndexing(store, databaseName: restoredDb);

            // no indexing errors after the restore and adding new document
            // legacy indexing behavior should be applied
            var indexErrors = store.Maintenance.ForDatabase(restoredDb).Send(new GetIndexErrorsOperation([index.IndexName, "Auto/Dtos/ByDtoInner"]));
            Assert.Equal(2, indexErrors.Length);
            Assert.Equal(0, indexErrors[0].Errors.Length);
            Assert.Equal(0, indexErrors[1].Errors.Length);

            // after index resets there are indexing errors on static index and no indexing errors on auto index
            store.Maintenance.ForDatabase(restoredDb).Send(new ResetIndexOperation(index.IndexName, indexResetMode: IndexResetMode.InPlace));
            store.Maintenance.ForDatabase(restoredDb).Send(new ResetIndexOperation("Auto/Dtos/ByDtoInner", indexResetMode: IndexResetMode.InPlace));
            Indexes.WaitForIndexing(store, databaseName: restoredDb, allowErrors: true);
            indexErrors = store.Maintenance.ForDatabase(restoredDb).Send(new GetIndexErrorsOperation([index.IndexName, "Auto/Dtos/ByDtoInner"]));
            try
            {
                Assert.Equal(3, indexErrors[0].Errors.Length);
            }
            catch (Exception e)
            {
                WaitForUserToContinueTheTest(store, database: restoredDb);

                Console.WriteLine(e);
                throw;
            }
            Assert.Contains("https://ravendb.net/l/OB9XW4/6.1", indexErrors[0].Errors[0].Error);
            Assert.Equal(0, indexErrors[1].Errors.Length);

            // force skipping indexing of complex fields
            index = new Index(complexFieldIndexingBehavior: IndexingConfiguration.CoraxComplexFieldIndexingBehavior.Skip);
            index.Execute(store, database: restoredDb);

            Indexes.WaitForIndexing(store, databaseName: restoredDb, allowErrors: true); // errors come from current static index, while we're replacing it by the one that won't have any errors

            indexErrors = store.Maintenance.ForDatabase(restoredDb).Send(new GetIndexErrorsOperation([index.IndexName]));
            Assert.Equal(0, indexErrors[0].Errors.Length);

        }
        void ExtractFile(string path)
        {
            using (var fileStream = File.Create(path))
            using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22534.RavenDB_22534.ravendb-snapshot"))
            {
                stream.CopyTo(fileStream);
            }
        }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index(IndexingConfiguration.CoraxComplexFieldIndexingBehavior? complexFieldIndexingBehavior = null)
        {
            Map = dtos => dtos.Select(x => new { x.DtoInner });

            if (complexFieldIndexingBehavior != null)
            {
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.CoraxStaticIndexComplexFieldIndexingBehavior)] =
                    complexFieldIndexingBehavior.ToString();
            }
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public DtoInner DtoInner { get; set; }
    }

    private class DtoInner
    {
        public string Name { get; set; }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(IndexingConfiguration.CoraxComplexFieldIndexingBehavior.Skip, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData(IndexingConfiguration.CoraxComplexFieldIndexingBehavior.Throw, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void StaticCoraxIndexMustRespectComplexFieldIndexingBehavior(Options options, IndexingConfiguration.CoraxComplexFieldIndexingBehavior complexFieldIndexingBehavior)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new Dto() { DtoInner = new DtoInner() { Name = "inner" } });
            s.SaveChanges();
        }
        var index = new Index(complexFieldIndexingBehavior);

        index.Execute(store);
        Indexes.WaitForIndexing(store, allowErrors: true);

        if (complexFieldIndexingBehavior == IndexingConfiguration.CoraxComplexFieldIndexingBehavior.Skip)
        {
            var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
            Assert.Null(errors);
        }
        else
        {
            var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: true);
            Assert.Equal(1, errors[0].Errors.Length);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void SampleDataIndexesShouldNotIndexComplexFields(Options options)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new CreateSampleDataOperation(DatabaseItemType.Documents | DatabaseItemType.Indexes | DatabaseItemType.CounterGroups));

        Indexes.WaitForIndexing(store);

        WaitForUserToContinueTheTest(store);

        var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation());

        foreach (var errors in indexErrors)
        {
            Assert.Empty(errors.Errors);
        }
    }
}
