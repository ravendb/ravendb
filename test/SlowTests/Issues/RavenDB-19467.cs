using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19467 : RavenTestBase
{
    private const string Term = "Kaszebe";
    
    public RavenDB_19467(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void WhenAutoIndexWithExactExistsRavenWillNotCreateAnotherIndexForSameField(bool shouldAddDocument)
    {
        using var store = PrepareStoreForTest(shouldAddDocument);
        //This should create Auto/Exact(Name)
        CreateAutoIndex(true);
        //This should not create any index but use Auto/Dtos/ByExact(Name) because Exact(Name) in backend is indexing two fields (with exact analyzer and standard)
        CreateAutoIndex(false);

        var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 25));
        Assert.Equal(1, indexes.Length);
        Assert.Equal("Auto/Dtos/ByExact(Name)", indexes[0].Name);
        
        void CreateAutoIndex(bool exact)
        {
            using var session = store.OpenSession();
            var result = session.Query<Dto>().Where(i => i.Name == Term, exact).ToList();
        }
    }
    
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AutoIndexWithoutExactWillBeDeletedAndRavenWillCreateNewIndexForBothFields(bool shouldAddDocument)
    {
        using var store = PrepareStoreForTest(shouldAddDocument);
        //This should create Auto/Dtis/Name
        CreateAutoIndex(false);
        var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 25));
        Assert.Equal(1, indexes.Length);
        Assert.Equal("Auto/Dtos/ByName", indexes[0].Name);
        
        //This should delete Auto/Dtos/Name and create Auto/Dtos/ByExact(Name)
        CreateAutoIndex(true);
        indexes = store.Maintenance.Send(new GetIndexesOperation(0, 25));
        Assert.Equal(1, indexes.Length);
        Assert.Equal("Auto/Dtos/ByExact(Name)", indexes[0].Name);
        
        //Should not create anything
        CreateAutoIndex(false);
        indexes = store.Maintenance.Send(new GetIndexesOperation(0, 25));
        Assert.Equal(1, indexes.Length);
        Assert.Equal("Auto/Dtos/ByExact(Name)", indexes[0].Name);
        
        void CreateAutoIndex(bool exact)
        {
            using var session = store.OpenSession();
            var result = session.Query<Dto>().Customize(i=> i.WaitForNonStaleResults()).Where(i => i.Name == Term, exact).ToList();
        }
    }

    private IDocumentStore PrepareStoreForTest(bool shouldAddDocument)
    {
        var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeBeforeDeletionOfSupersededAutoIndex)] = "0";
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle)] = "0";
            }
        });

        if (shouldAddDocument)
        {
            using var session = store.OpenSession();
            session.Store(new Dto() {Name = Term});
            session.SaveChanges();
        }

        return store;
    }
    
    private class Dto
    {
        public string Name { get; set; }
    }
}
