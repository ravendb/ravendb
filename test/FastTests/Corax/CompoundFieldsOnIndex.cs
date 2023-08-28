using System;
using System.Linq;
using System.Threading.Tasks;
using Corax.Queries;
using Corax.Queries.SortingMatches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CompoundFieldsOnIndex : RavenTestBase
{
    public CompoundFieldsOnIndex(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void CanDefineIndexWithCompoundFieldAndReadItBack()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var index = new Users_Idx();
        index.Execute(store);
        var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
        Assert.NotEmpty(definition.CompoundFields);
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanIndexWithCompoundField()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var index = new Users_Idx();
        index.Execute(store);
        using (var s = store.OpenSession())
        {
            s.Store(new User("Corax", "Hadera", new DateTime(2014, 4, 1)));
            s.Store(new User("aCorax", "Hader", new DateTime(2014, 4, 1)));
            s.Store(new User("Lucene", "Hadera", new DateTime(2009, 4, 1)));

            s.Store(new User("Corax", "Torun", new DateTime(2021, 4, 1)));
            s.SaveChanges();
        }
        Indexes.WaitForIndexing(store);
        using (var s = store.OpenSession())
        {
            var users = s.Query<User, Users_Idx>()
                .Where(x => x.Name == "Corax")
                .OrderBy(x => x.Birthday)
                .ToList();
            Assert.Equal(2, users.Count);
            Assert.Equal(2014, users[0].Birthday.Year);
            Assert.Equal(2021, users[1].Birthday.Year);
        }
        
        using (var s = store.OpenSession())
        {
            var users = s.Query<User, Users_Idx>()
                .Where(x => x.Location == "Hadera")
                .OrderBy(x => x.Name)
                .ToList();
            Assert.Equal(2, users.Count);
            Assert.Equal("Corax", users[0].Name);
            Assert.Equal("Lucene", users[1].Name);
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task CanOptimizeToSkipSorting()
    {
        await TestQueryBuilder<MultiTermMatch>(s => s.Advanced.AsyncDocumentQuery<User, Users_Idx>()
            .WhereEquals(x => x.Location, "Hadera")
            .OrderBy(x => x.Name)
            .GetIndexQuery()
        );
        await TestQueryBuilder<MultiTermMatch>(s => s.Advanced.AsyncDocumentQuery<User, Users_Idx>()
            .WhereEquals(x => x.Name, "Corax")
            .OrderBy(x => x.Birthday)
            .GetIndexQuery()
        );
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex()
    {
        await TestQueryBuilder<SortingMatch>(s => s.Advanced.AsyncDocumentQuery<User, Users_Idx>()
            .WhereEquals(x => x.Name, "Lucene")
            .AndAlso()
            .WhereEquals(x=>x.Location, "Hadera")
            .OrderBy(x => x.Birthday)
            .GetIndexQuery()
        );
        
        await TestQueryBuilder<SortingMatch>(s => s.Advanced.AsyncDocumentQuery<User, Users_Idx>()
            .WhereEquals(x => x.Name, "Lucene")
            .OrderBy(x => x.Location)
            .GetIndexQuery()
        );
    }

    private Task TestQueryBuilder<TExpected>(Func<IAsyncDocumentSession, IndexQuery> query)
    {
        return StreamingOptimization_QueryBuilder.TestQueryBuilder<TExpected,Users_Idx>(this, false, query);
    }


    private record User(string Name, string Location, DateTime Birthday);

    private class Users_Idx : AbstractIndexCreationTask<User>
    {
        public Users_Idx()
        {
            Map = users => 
                from u in users 
                select new { u.Name, u.Location, u.Birthday };

            CompoundFields.Add(new[] { nameof(User.Name), nameof(User.Birthday) });
            CompoundFields.Add(new[] { nameof(User.Location), nameof(User.Name) });
        }
    }
    
    
}
