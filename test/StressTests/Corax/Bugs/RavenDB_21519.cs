using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Corax.Bugs;

public class RavenDB_21519 : RavenTestBase
{
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void PostingListUpdateDocumentWithBiggerFrequencyButTheSameAsAlreadyIndexedAfterQuantization(Options options)
    {
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenSession())
        {
            var results = session.Query<User>()
                .Search(x => x.Text, "maciej")
                .ToList();
            session.Store(new User(TextGen(10)), "doc1");
            session.Store(new User(TextGen(100)), "doc2");
            session.SaveChanges();
        }

        using (var bulkInsert = store.BulkInsert())
        {
            foreach (var i in Enumerable.Range(0, 20_000))
                bulkInsert.Store(new User("maciej"), $"doc{i + 4}");
        }

        Indexes.WaitForIndexing(store);
        
        using (var session = store.OpenSession())
        {
            var prevUser = session.Load<User>("doc2");
            prevUser.Text = TextGen(101); //changing reference, there will be update
            session.Store(new User(TextGen(10)), "doc3");
            session.SaveChanges();
        }        
        
        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var results = session.Query<User>()
                .Search(x => x.Text, "maciej")
                .Count();
            
            Assert.Equal(20_00_3, results);
        }
        
        
        string TextGen(int count) => string.Join(" ", Enumerable.Range(0, count).Select(_ => "Maciej"));
    }

    public RavenDB_21519(ITestOutputHelper output) : base(output)
    {
    }
    
    private class User
    {
        public User()
        {
        }

        public User(string text)
        {
            Text = text;
        }

        public string Id;
        public string Text;
    }
}
