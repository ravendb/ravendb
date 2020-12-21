// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.Streaming
{
    public class QueryStreaming : RavenTestBase
    {
        public QueryStreaming(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStreamQueryResults()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                int count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, Users_ByName>();

                    var reader = session.Advanced.Stream(query);

                    while (reader.MoveNext())
                    {
                        count++;
                        Assert.IsType<User>(reader.Current.Document);
                    }
                }
                Assert.Equal(200, count);
                count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User, Users_ByName>();
                    var reader = session.Advanced.Stream(query);
                    while (reader.MoveNext())
                    {
                        count++;
                        Assert.IsType<User>(reader.Current.Document);

                    }
                }

                Assert.Equal(200, count);
            }
        }

        [Fact]
        public void CanStreamQueryResultsWithQueryStatistics()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, Users_ByName>();

                    StreamQueryStatistics stats;
                    var reader = session.Advanced.Stream(query, out stats);

                    while (reader.MoveNext())
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal("Users/ByName", stats.IndexName);
                    Assert.Equal(100, stats.TotalResults);
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User, Users_ByName>();
                    StreamQueryStatistics stats;
                    var reader = session.Advanced.Stream(query, out stats);

                    while (reader.MoveNext())
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal("Users/ByName", stats.IndexName);
                    Assert.Equal(100, stats.TotalResults);
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year); 
                }
            }
        }

        [Fact]
        public async Task CanStreamQueryResultsWithQueryStatisticsAsync()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new User()).ConfigureAwait(false);
                    }
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User, Users_ByName>();

                    StreamQueryStatistics stats;
                    var reader = await session.Advanced.StreamAsync(query, out stats).ConfigureAwait(false);

                    while (await reader.MoveNextAsync().ConfigureAwait(false))
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal("Users/ByName", stats.IndexName);
                    Assert.Equal(100, stats.TotalResults);
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncDocumentQuery<User, Users_ByName>();
                    StreamQueryStatistics stats;
                    var reader = await session.Advanced.StreamAsync(query, out stats);

                    while (await reader.MoveNextAsync().ConfigureAwait(false))
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal(stats.IndexName, "Users/ByName");
                    Assert.Equal(stats.TotalResults, 100);
                    Assert.Equal(stats.IndexTimestamp.Year, DateTime.Now.Year);
                }
            }
        }

        private class MyClass
        {
            public string Prop1 { get; set; }
            public string Prop2 { get; set; }
            public int Index { get; set; }
        }

        [Fact]
        public void TestFailingProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new MyClass { Index = 1, Prop1 = "prop1", Prop2 = "prop2" });
                    session.Store(new MyClass { Index = 2, Prop1 = "prop1", Prop2 = "prop2" });
                    session.Store(new MyClass { Index = 3, Prop1 = "prop1", Prop2 = "prop2" });

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var indexDef = new IndexDefinitionBuilder<MyClass>()
                    {
                        Map = docs => from doc in docs select new { Index = doc.Index }
                    };

                    var indexDefinition = indexDef.ToIndexDefinition(store.Conventions, true);
                    indexDefinition.Name = "MyClass/ByIndex";
                    store.Maintenance.Send(new PutIndexesOperation(new[] { indexDefinition }));

                    WaitForIndexing(store);

                    var query = session.Query<MyClass>("MyClass/ByIndex")
                        .Select(x => new MyClass
                        {
                            Index = x.Index,
                            Prop1 = x.Prop1
                        });

                    var enumerator = session.Advanced.Stream(query);
                    int count = 0;
                    while (enumerator.MoveNext())
                    {
                        Assert.IsType<MyClass>(enumerator.Current.Document);
                        Assert.Null(((MyClass)enumerator.Current.Document).Prop2);
                        count++;
                    }

                    Assert.Equal(3, count);
                }
            }
        }

        class TestObj
        {
            public string Id { get; set; }
            public Dictionary<string, string> Prop { get; set; }
        }
        
        [Fact]
        public async Task QueryStream_WhenDocsContainsMultipleUniqPropertyNames_ShouldNotBeVeryVerySlow()
        {
            using var store = GetDocumentStore();

            var objs = Enumerable.Range(0, 100000).Select(_ => new TestObj
            {
                Prop = new Dictionary<string, string>
                {
                    {Guid.NewGuid().ToString("n"), "someValue"},
                    {Guid.NewGuid().ToString("n"), "someValue"},
                    {Guid.NewGuid().ToString("n"), "someValue"}
                }
            });

            {
                // After solving RavenDB-16040/RavenDB-16039 the iteration can be simplified 
                var session = store.OpenAsyncSession();
                try
                {
                    var i = 0;
                    foreach (var obj in objs)
                    {
                        await session.StoreAsync(obj);
                        i++;
                        if (i % 100 == 0)
                        {
                            await session.SaveChangesAsync();
                            session.Dispose();
                            session = store.OpenAsyncSession();
                        }
                    }
                    await session.SaveChangesAsync();
                }
                finally
                {
                    session.Dispose();
                }
            }

            await Assert(Task.Run(() =>
            {
                using var session = store.OpenSession();
                using (var stream = session.Advanced.Stream(session.Query<dynamic>()))
                {
                    while (true)
                    {
                        if (stream.MoveNext() == false)
                            break;
                    }
                }
            }));

            await Assert(Task.Run(async () =>
            {
                using var session = store.OpenAsyncSession();
                await using(var stream = await session.Advanced.StreamAsync(session.Query<dynamic>()))
                {
                    while (true)
                    {
                        if (await stream.MoveNextAsync() == false)
                            break;
                    }
                }
            }));

            static async Task Assert(Task test)
            {
                await Task.WhenAny(test, Task.Delay(TimeSpan.FromMinutes(2)));
                Xunit.Assert.True(test.IsCompletedSuccessfully);
            }
        }
        
        [Fact]
        public void Streaming_Results_Should_Sort_Properly()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new FooIndex());

                using (var session = documentStore.OpenSession())
                {
                    var random = new System.Random();

                    for (int i = 0; i < 100; i++)
                        session.Store(new Foo { Num = random.Next(1, 100) });

                    session.SaveChanges();
                }

                WaitForIndexing(documentStore);


                Foo last = null;

                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<Foo, FooIndex>().OrderBy(x => x.Num);

                    var enumerator = session.Advanced.Stream(q);

                    while (enumerator.MoveNext())
                    {
                        Foo foo = (Foo)enumerator.Current.Document;
                        Debug.WriteLine("{0} - {1}", foo.Id, foo.Num);


                        if (last != null)
                        {
                            // If the sort worked, this test should pass
                            Assert.True(last.Num <= foo.Num);
                        }

                        last = foo;

                    }
                }
            }
        }

        private class Foo
        {
            public string Id { get; set; }
            public int Num { get; set; }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public FooIndex()
            {
                Map = foos => from foo in foos
                              select new { foo.Num };

            }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from u in users select new { Name = u.Name, LastName = u.LastName.Boost(10) };

                Indexes.Add(x => x.Name, FieldIndexing.Search);

                IndexSuggestions.Add(x => x.Name);

                Analyzers.Add(x => x.Name, typeof(Lucene.Net.Analysis.SimpleAnalyzer).FullName);

                Stores.Add(x => x.Name, FieldStorage.Yes);
            }
        }
    }
}
