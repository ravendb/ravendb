using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22406 : RavenTestBase
{
    public RavenDB_22406(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanIndexProperlyWithOneInvalidDocument(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new FanoutDto() { Inners = new[] { new Inner(false, 1), new Inner(false, 2) } });
        session.Store(new FanoutDto() { Inners = new[] { new Inner(false, 3), new Inner(false, 4), new Inner(true, 5) } });
        session.Store(new FanoutDto() { Inners = new[] { new Inner(false, 6), new Inner(false, 7) } });
        new FanoutIndex().Execute(store);
        session.Advanced.WaitForIndexesAfterSaveChanges();
        session.SaveChanges();

        var count = session.Query<FanoutDto, FanoutIndex>().Count();
        Assert.Equal(4, count); //doc1 and doc3 only
        var terms = store
            .Maintenance
            .Send(new GetTermsOperation(new FanoutIndex().IndexName, "id()", null, int.MaxValue));

        Assert.Equal(2, terms.Length);
    }

    private class FanoutDto
    {
        public Inner[] Inners { get; set; }
    }

    private record Inner(bool Throw, decimal Value);

    private class FanoutIndex : AbstractIndexCreationTask<FanoutDto>
    {
        public FanoutIndex()
        {
            Map = dtos => from dto in dtos
                from inner in dto.Inners
                select new { Alphabet = inner.Value.ToString(), Value = inner.Throw ? inner.Value / 0 : inner.Value };
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void BackwardCompatibilityComplexFields()
    {  
        var backupPath = NewDataPath(forceCreateDir: true);
        var file = Path.Combine(backupPath, "RavenDB_22406.ravendb-snapshot");
        ExtractFile(file);
       
        var db = GetDatabaseName();
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = db});
        
        using (var session = store.OpenSession(db))
        {
            session.Advanced.WaitForIndexesAfterSaveChanges();
            var index = new Index();
            session.Store(new Dto() { Id = "doc3", DtoInner = new DtoInner() { Name = "3inner" } });
            session.SaveChanges();
            Indexes.WaitForIndexing(store);
            
            var indexErrors = store.Maintenance.ForDatabase(db).Send(new GetIndexErrorsOperation([index.IndexName, "Auto/Dtos/ByDtoInner"]));
            Assert.Equal(2, indexErrors.Length);
            Assert.Equal(0, indexErrors[0].Errors.Length);
            Assert.Equal(0, indexErrors[1].Errors.Length);

            store.Maintenance.ForDatabase(db).Send(new ResetIndexOperation(index.IndexName, indexResetMode: IndexResetMode.InPlace));
            store.Maintenance.ForDatabase(db).Send(new ResetIndexOperation("Auto/Dtos/ByDtoInner", indexResetMode: IndexResetMode.InPlace));
            Indexes.WaitForIndexing(store, databaseName: db, allowErrors: true);
            indexErrors = store.Maintenance.ForDatabase(db).Send(new GetIndexErrorsOperation([index.IndexName, "Auto/Dtos/ByDtoInner"]));
            Assert.Equal(3, indexErrors[0].Errors.Length);
            Assert.Contains("https://ravendb.net/l/OB9XW4/6.0/Csharp", indexErrors[0].Errors[0].Error);
            Assert.Equal(0, indexErrors[1].Errors.Length);
        }
        
        void ExtractFile(string path)
        {
            using (var fileStream = File.Create(path))
            using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22406.RavenDB_22406.ravendb-snapshot"))
            {
                stream.CopyTo(fileStream);
            }
        }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => dtos.Select(x => new { x.DtoInner });
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
}
