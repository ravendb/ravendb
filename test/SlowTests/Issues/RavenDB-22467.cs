using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22467 : RavenTestBase
{
    public RavenDB_22467(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestIfTakeClauseLimitsNumberOfReturnedDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var u1 = new User() { Id = "user/1", Name = "CoolName", Note = "some note" };
                var u2 = new User() { Id = "user/2", Name = "CoolName", Note = "some note" };
                var u3 = new User() { Id = "user/3", Name = "CoolName", Note = "some note" };
                var u4 = new User() { Id = "user/4", Name = "CoolName", Note = "some note" };
                var u5 = new User() { Id = "user/5", Name = "CoolName", Note = "some note" };
                
                session.Store(u1);
                session.Store(u2);
                session.Store(u3);
                session.Store(u4);
                session.Store(u5);
                
                session.SaveChanges();

                var index = new UserMoreLikeThisIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var moreUsers = session.Query<User, UserMoreLikeThisIndex>()
                    .MoreLikeThis(builder => builder
                        .UsingDocument(x => x.Id == u1.Id)
                        .WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Name", "Note" },
                        }))
                    .Take(3)
                    .ToList();

                Assert.Equal(3, moreUsers.Count);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestIfTakeClauseLimitsNumberOfReturnedDocumentsWithMultipleCoraxFills(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 6000; i++)
                {
                    session.Store(new User(){ Id = $"user/{i + 1}", Name = "CoolName", Note = "SomeNote" });
                }
                
                session.SaveChanges();

                var index = new UserMoreLikeThisIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var moreUsers = session.Query<User, UserMoreLikeThisIndex>()
                    .MoreLikeThis(builder => builder
                        .UsingDocument(x => x.Id == "user/1")
                        .WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Name", "Note" },
                        }))
                    .Take(5000)
                    .ToList();

                Assert.Equal(5000, moreUsers.Count);
                
                moreUsers = session.Query<User, UserMoreLikeThisIndex>()
                    .MoreLikeThis(builder => builder
                        .UsingDocument(x => x.Id == "user/1")
                        .WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Name" },
                        }))
                    .Take(3)
                    .ToList();
                
                Assert.Equal(3, moreUsers.Count);
                
                moreUsers = session.Query<User, UserMoreLikeThisIndex>()
                    .MoreLikeThis(builder => builder
                        .UsingDocument(x => x.Id == "user/1"))
                    .Take(254)
                    .ToList();
                
                Assert.Equal(254, moreUsers.Count);
                
                moreUsers = session.Query<User, UserMoreLikeThisIndex>()
                    .MoreLikeThis(builder => builder
                        .UsingDocument(x => x.Id == "user/1"))
                    .Take(99999)
                    .ToList();
                
                Assert.Equal(5999, moreUsers.Count);
            }
        }
    }
    
    private class UserMoreLikeThisIndex : AbstractIndexCreationTask<User>
    {
        public UserMoreLikeThisIndex()
        {
            Map = users => from user in users
                select new
                {
                    user.Name,
                    user.Note
                };

            Indexes.Add(x => x.Name, FieldIndexing.Search);
            Indexes.Add(x => x.Note, FieldIndexing.Search);
            
            Stores.Add(x => x.Name, FieldStorage.Yes);
            Stores.Add(x => x.Note, FieldStorage.Yes);
        }
    }
    
    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Note { get; set; }
    }
}
