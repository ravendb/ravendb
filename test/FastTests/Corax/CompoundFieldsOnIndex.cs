using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Corax.Querying.Matches;
using Corax.Querying.Matches.SortingMatches;
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
    public void CanDefineIndexWithCompoundFieldAndReadItBack() => CanDefineIndexWithCompoundFieldAndReadItBack<Users_Idx>();

    [RavenFact(RavenTestCategory.Indexes)]
    public void CanDefineIndexWithCompoundFieldAndReadItBackMapReduce() => CanDefineIndexWithCompoundFieldAndReadItBack<MapReduceUsers_Idx>();
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanDefineIndexWithCompoundFieldAndReadItBackJavaScript() => CanDefineIndexWithCompoundFieldAndReadItBack<Js_Users_Idx>();
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanDefineIndexWithCompoundFieldAndReadItBackMapReduceJavaScript() => CanDefineIndexWithCompoundFieldAndReadItBack<MapReduceUsers_Idx>();
    
    private void CanDefineIndexWithCompoundFieldAndReadItBack<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var index = new TIndex();
        index.Execute(store);
        var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
        Assert.NotEmpty(definition.CompoundFields);
    }


    [RavenFact(RavenTestCategory.Indexes)]
    public void CanIndexWithCompoundField() => CanIndexWithCompoundField<Users_Idx>();
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanIndexWithCompoundFieldMapReduce() => CanIndexWithCompoundField<MapReduceUsers_Idx>();

    [RavenFact(RavenTestCategory.Indexes)]
    public void CanIndexWithCompoundFieldJavaScript() => CanIndexWithCompoundField<Js_Users_Idx>();
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanIndexWithCompoundFieldJavaScriptMapReduce() => CanIndexWithCompoundField<MapReduceJs_Users_Idx>();
    
    private void CanIndexWithCompoundField<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var index = new TIndex();
        var i2 = new Users_Idx();
        i2.Execute(store);
        index.Execute(store);
        using (var s = store.OpenSession())
        {
            s.Store(new User("Corax", "Hadera", new DateTime(2014, 4, 1)));
            s.Store(new User("aCorax", "Hader", new DateTime(2014, 4, 1)));
            s.Store(new User("Lucene", "Hadera", new DateTime(2009, 4, 1)));
        
            s.Store(new User("Corax", "Torun", new DateTime(2021, 4, 1)));
            
            s.Store(new User("A", "IL", new DateTime(2014, 4, 1)));
            s.Store(new User("B", "IL", new DateTime(2014, 4, 1)));
            s.Store(new User("A", "IL", new DateTime(2009, 4, 1)));
        
            
            s.SaveChanges();
        }
         Indexes.WaitForIndexing(store);
        // using (var s = store.OpenSession())
        // {
        //     var users = s.Query<User, TIndex>()
        //         .Where(x => x.Name == "Corax")
        //         .OrderBy(x => x.Birthday)
        //         .ToList();
        //     WaitForUserToContinueTheTest(store);
        //
        //     Assert.Equal(2, users.Count);
        //     Assert.Equal(2014, users[0].Birthday.Year);
        //     Assert.Equal(2021, users[1].Birthday.Year);
        // }
        
        using (var s = store.OpenSession())
        {
            var users = s.Query<User, TIndex>()
                .Where(x => x.Location == "IL")
                .OrderBy(x => x.Name)
                .ThenBy(x=>x.Birthday)
                .ToList();
            Assert.Equal(3, users.Count);
            Assert.Equal(2009, users[0].Birthday.Year);
            Assert.Equal(2014, users[1].Birthday.Year);
            Assert.Equal(2014, users[2].Birthday.Year);
            
            Assert.Equal("A", users[1].Name);
            Assert.Equal("B", users[2].Name);
        }
        
        using (var s = store.OpenSession())
        {
            var users = s.Query<User, TIndex>()
                .Where(x => x.Location == "Hadera")
                .OrderBy(x => x.Name)
                .ToList();
            Assert.Equal(2, users.Count);
            Assert.Equal("Corax", users[0].Name);
            Assert.Equal("Lucene", users[1].Name);
        }
        
        WaitForUserToContinueTheTest(store);
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task CanOptimizeToSkipSorting() => await CanOptimizeToSkipSorting<Users_Idx>();
    
    [RavenFact(RavenTestCategory.Querying)]
    public async Task CanOptimizeToSkipSortingMapReduce() => await CanOptimizeToSkipSorting<MapReduceUsers_Idx>();
    
    [RavenFact(RavenTestCategory.Querying)]
    public async Task CanOptimizeToSkipSortingJavaScript() => await CanOptimizeToSkipSorting<Js_Users_Idx>();
    
    [RavenFact(RavenTestCategory.Querying)]
    public async Task CanOptimizeToSkipSortingJavaScriptMapReduce() => await CanOptimizeToSkipSorting<MapReduceJs_Users_Idx>();
    
    private async Task CanOptimizeToSkipSorting<TIndex>()  where TIndex : AbstractIndexCreationTask, new()
    {
        await TestQueryBuilder<MultiTermMatch, TIndex>(s => s.Advanced.AsyncDocumentQuery<User, TIndex>()
            .WhereEquals(x => x.Location, "Hadera")
            .OrderBy(x => x.Name)
            .GetIndexQuery()
        );
        await TestQueryBuilder<MultiTermMatch, TIndex>(s => s.Advanced.AsyncDocumentQuery<User, TIndex>()
            .WhereEquals(x => x.Name, "Corax")
            .OrderBy(x => x.Birthday)
            .GetIndexQuery()
        );
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex() => await Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex<Users_Idx>();
    
    [RavenFact(RavenTestCategory.Querying)]
    public async Task Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndexMapReduce() => await Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex<MapReduceUsers_Idx>();
    
    [RavenFact(RavenTestCategory.Querying)]
    public async Task Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex_JavaScript() => await Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex<Js_Users_Idx>();

    [RavenFact(RavenTestCategory.Querying)]
    public async Task Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex_JavaScriptMapReduce() => await Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex<MapReduceUsers_Idx>();

    private async Task Will_NOT_OptimizeQueryIfThereIsNoMatchingCompoundIndex<TIndex>()  where TIndex : AbstractIndexCreationTask, new()
    {
        await TestQueryBuilder<SortingMatch, TIndex>(s => s.Advanced.AsyncDocumentQuery<User, TIndex>()
            .WhereEquals(x => x.Name, "Lucene")
            .AndAlso()
            .WhereEquals(x=>x.Location, "Hadera")
            .OrderBy(x => x.Birthday)
            .GetIndexQuery()
        );
        
        await TestQueryBuilder<SortingMatch, TIndex>(s => s.Advanced.AsyncDocumentQuery<User, TIndex>()
            .WhereEquals(x => x.Name, "Lucene")
            .OrderBy(x => x.Location)
            .GetIndexQuery()
        );
        
        await TestQueryBuilder<SortingMultiMatch, TIndex>(s => s.Advanced.AsyncDocumentQuery<User, TIndex>()
            .WhereEquals(x => x.Location, "Hadera")
            .OrderBy(nameof(User.Name))
            .OrderBy(nameof(User.Birthday))
            .GetIndexQuery()
        );
    }

    private Task TestQueryBuilder<TExpected, TIndex>(Func<IAsyncDocumentSession, IndexQuery> query)  where TIndex : AbstractIndexCreationTask, new()
    {
        return StreamingOptimization_QueryBuilder.TestQueryBuilder<TExpected,TIndex>(this, false, query);
    }


    private record User(string Name, string Location, DateTime Birthday, string Id = null);

    private class MapReduceUsers_Idx : AbstractIndexCreationTask<User, User>
    {
        public MapReduceUsers_Idx()
        {
            Map  = users => 
                from u in users 
                select new { u.Name, u.Location, u.Birthday, u.Id };

            Reduce = users => from u in users
                group u by u.Id
                into g
                select new {g.First().Name, g.First().Location, g.First().Birthday, g.First().Id};
            
            CompoundField("Name", "Birthday");
            CompoundField("Location", "Name");
        }
    }
    
    private class Users_Idx : AbstractIndexCreationTask<User>
    {
        public Users_Idx()
        {
            Map = users => 
                from u in users 
                select new { u.Name, u.Location, u.Birthday };

            CompoundField("Name", "Birthday");
            CompoundField("Location", "Name");
        }
    }
    
    private class Js_Users_Idx : AbstractJavaScriptIndexCreationTask
    {
        public Js_Users_Idx()
        {
            Maps = new HashSet<string>() { @"map('Users', (user) => {
                    return {
                        Name: user.Name,
                        Location: user.Location,
                        Birthday: user.Birthday
                    };
})"};
            
            CompoundField("Name", "Birthday");
            CompoundField("Location", "Name");
        }
    }
    
    private class MapReduceJs_Users_Idx : AbstractJavaScriptIndexCreationTask
    {
        public MapReduceJs_Users_Idx()
        {
            Maps = new HashSet<string>() { @"map('Users', (user) => {
                    return {
                        Name: user.Name,
                        Location: user.Location,
                        Birthday: user.Birthday,
                        Id: id(user)
                    };
})"};
            
            Reduce = @"groupBy(x => ( { Id: x.Id } ))
                                .aggregate(g => {return {
                                    Name: g.values[0].Name,
                                    Location: g.values[0].Location,
                                    Birthday: g.values[0].Birthday,
                                    Id: g.values[0].Id
                               };})";
            
            CompoundField("Name", "Birthday");
            CompoundField("Location", "Name");
        }
    }
}
