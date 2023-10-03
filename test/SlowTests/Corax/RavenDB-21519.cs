using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public sealed class RavenDB_21519 : RavenTestBase
{
    public RavenDB_21519(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void UpdateDocumentWithBiggerFrequencyButTheSameAsAlreadyIndexedAfterQuantization(Options options)
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
            
            Assert.Equal(3, results);
        }
        
        
        string TextGen(int count) => string.Join(" ", Enumerable.Range(0, count).Select(_ => "Maciej"));
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData(956465115)]
    public void Fuzzy(int seed)
    {
        var random = new Random(seed);
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        var operationCount = random.Next(1, 4);
        var existingIds = new List<string>();

        using (var _ = store.OpenSession())
        {
            var results = _.Query<User>()
                .Search(x => x.Text, "maciej")
                .ToList();
        }

        while (operationCount-- >= 0)
        {
            Indexes.WaitForIndexing(store);
            using var session = store.OpenSession();
            session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

            var updatedDoNotTouch = new HashSet<string>();
            var users = new List<User>();
            var docsToHandleInSession = random.Next(0, 256);


            for (int idX = 0; idX < docsToHandleInSession; ++idX)
            {
                var action = (Action)(random.Next() % 3 + 1);
                switch (action)
                {
                    case Action.Add:
                        var user = new User(TextGen(random.Next(0, 128)));
                        users.Add(user);
                        session.Store(user);
                        break;
                    case Action.Remove:
                        {
                            if (existingIds.Count == 0)
                                break;
                            var index = random.Next(existingIds.Count);
                            var toDelete = existingIds[index];

                            var doc = session.Load<User>(toDelete);

                            session.Delete(doc);
                            existingIds.RemoveAt(index);
                            break;
                        }
                    case Action.Update:
                        {
                            if (existingIds.Count == 0)
                                break;

                            var index = random.Next(existingIds.Count);
                            var toUpdate = existingIds[index];

                            if (updatedDoNotTouch.Contains(toUpdate))
                                break;

                            var doc = session.Load<User>(toUpdate);
                            doc.Text = TextGen(random.Next(0, 128));
                            session.Store(doc);
                            updatedDoNotTouch.Add(toUpdate);
                            break;
                        }

                }
            }
            
            session.SaveChanges();
            foreach (var newUsers in users)
                existingIds.Add(newUsers.Id);

            Indexes.WaitForIndexing(store);

            var currentCount = session.Query<User>()
                .Search(x => x.Text, "maciej")
                .Count();
        }

        string TextGen(int count) => string.Join(" ", Enumerable.Range(0, count).Select(_ => "Maciej"));
    }

    private enum Action : byte
    {
        Add = 1,
        Remove = 2,
        Update = 3
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
