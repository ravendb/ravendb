using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public sealed class RavenDB_21498 : RavenTestBase
{
    public RavenDB_21498(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void ProperlyUpdateSmallPostingListWithDifferentFrequencies()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var session = store.OpenSession())
        {
            session.Store(new User(TextGen(16)), "doc/1");
            session.SaveChanges();

            var results = session.Query<User>()
                .Search(x => x.Text, "maciej")
                .ToList();
            
            Indexes.WaitForIndexing(store);
            
            session.Store(new User(TextGen(10)), "doc/2");
            session.SaveChanges();

            Indexes.WaitForIndexing(store);

            session.Store(new User(TextGen(11)), "doc/3");
            session.SaveChanges();

            results = session.Query<User>()
                .Customize(x => x.WaitForNonStaleResults())
                .Search(x => x.Text, "maciej")
                .ToList();
            
            Assert.Equal(3, results.Count);
        }

        using (var session = store.OpenSession())
        {
            var userToUpdate = session.Load<User>("doc/1");
            userToUpdate.Text = TextGen(24);
            session.Store(userToUpdate);
            session.Store(new User(TextGen(11)), "doc/4");

            session.SaveChanges();

            Indexes.WaitForIndexing(store);

            var results = session.Query<User>()
              .Search(x => x.Text, "maciej")
                .Count(); // will do not check by Raven's ids but results from Corax :
            
            Assert.Equal(4, results);
        }
        

        string TextGen(int count) => string.Join(" ", Enumerable.Range(0, count).Select(_ => "Maciej"));
    }
    
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void ProperlyUpdateSmallPostingListWithDifferentFrequencies2()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var session = store.OpenSession())
        {
            session.Store(new User(TextGen(32)), "doc/1");
            session.Store(new User(TextGen(32)), "doc/2");
            session.SaveChanges();

            var results = session.Query<User>()
                .Search(x => x.Text, "maciej")
                .ToList();
            
            Indexes.WaitForIndexing(store);
            
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var userToUpdate = session.Load<User>("doc/1");
            userToUpdate.Text = TextGen(64);
            session.Store(userToUpdate);
            session.SaveChanges();

            Indexes.WaitForIndexing(store);

            var results = session.Query<User>()
                .Search(x => x.Text, "maciej")
                .Count(); // will do not check by Raven's ids but results from Corax :
            
            Assert.Equal(2, results);
        }
        

        string TextGen(int count) => string.Join(" ", Enumerable.Range(0, count).Select(_ => "Maciej"));
    }


    private sealed class User 
    {
        public User(string text)
        {
            Text = text;
        }
        
        public string Text { get; set; }
        public string Id { get; set; } 
    }
}
