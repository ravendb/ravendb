// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using SlowTests.Core.Session;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.Streaming
{
    public class QueryStreaming : RavenTestBase
    {
        public QueryStreaming(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CanStreamQueryResults_Lucene(Options options) => CanStreamQueryResults<Users_ByName>(options);
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CanStreamQueryResults_Corax(Options options) => CanStreamQueryResults<Users_ByName_WithoutBoosting>(options);
        
        private void CanStreamQueryResults<TIndex>(Options options) where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                new TIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                int count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, TIndex>();

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
                    var query = session.Advanced.DocumentQuery<User, TIndex>();
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
        public void CanStreamQueryResults_CustomizeAfterStreamExecuted()
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

                Indexes.WaitForIndexing(store);

                long count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session
                        .Query<User, Users_ByName>()
                        .Customize(x => x
                            .AfterStreamExecuted(streamResult =>
                            {
                                count++;
                            }));

                    var reader = session.Advanced.Stream(query);

                    while (reader.MoveNext())
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }
                }
                
                Assert.Equal(200, count);
                count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .DocumentQuery<User, Users_ByName>()
                        .AfterStreamExecuted(streamResult =>
                        {
                            count++;
                        });
                    
                    var reader = session.Advanced.Stream(query);
                    while (reader.MoveNext())
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }
                }

                Assert.Equal(200, count);
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void CanStreamQueryResultsWithQueryStatistic_Corax(Options options) => CanStreamQueryResultsWithQueryStatistics<Users_ByName_WithoutBoosting>(options, "Users/ByName/WithoutBoosting");

        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CanStreamQueryResultsWithQueryStatistic_Lucene(Options options) => CanStreamQueryResultsWithQueryStatistics<Users_ByName>(options, "Users/ByName");
        
        private void CanStreamQueryResultsWithQueryStatistics<TIndex>(Options options, string indexName) where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                new TIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, TIndex>();

                    StreamQueryStatistics stats;
                    var reader = session.Advanced.Stream(query, out stats);

                    while (reader.MoveNext())
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal(indexName, stats.IndexName);
                    Assert.Equal(100, stats.TotalResults);
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<User, TIndex>();
                    StreamQueryStatistics stats;
                    var reader = session.Advanced.Stream(query, out stats);

                    while (reader.MoveNext())
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal(indexName, stats.IndexName);
                    Assert.Equal(100, stats.TotalResults);
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year); 
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamQueryResultsWithQueryStatisticsAsync_Lucene(Options options) =>
            await CanStreamQueryResultsWithQueryStatisticsAsync<Users_ByName>(options, "Users/ByName");
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanStreamQueryResultsWithQueryStatisticsAsync_Corax(Options options) =>
            await CanStreamQueryResultsWithQueryStatisticsAsync<Users_ByName_WithoutBoosting>(options, "Users/ByName/WithoutBoosting");

        private async Task CanStreamQueryResultsWithQueryStatisticsAsync<TIndex>(Options options, string indexName) where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                new TIndex().Execute(store);

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new User()).ConfigureAwait(false);
                    }
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }

                await Indexes.WaitForIndexingAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User, TIndex>();

                    StreamQueryStatistics stats;
                    var reader = await session.Advanced.StreamAsync(query, out stats).ConfigureAwait(false);

                    while (await reader.MoveNextAsync().ConfigureAwait(false))
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal(indexName, stats.IndexName);
                    Assert.Equal(100, stats.TotalResults);
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced.AsyncDocumentQuery<User, TIndex>();
                    StreamQueryStatistics stats;
                    var reader = await session.Advanced.StreamAsync(query, out stats);

                    while (await reader.MoveNextAsync().ConfigureAwait(false))
                    {
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal(stats.IndexName, indexName);
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void TestFailingProjection(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                    Indexes.WaitForIndexing(store);

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

        [Theory]
        [RavenExplicitData]
        public async Task QueryStream_WhenDocsContainsMultipleUniqPropertyNames_ShouldNotBeVeryVerySlow(RavenTestParameters config)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindClrType = (t, b) => "SomeType",
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                }
            });

            var objs = Enumerable.Range(0, 50000).Select(_ => new Dictionary<string, string>
            {
                [Guid.NewGuid().ToString("n")] = "someValue",
                [Guid.NewGuid().ToString("n")] = "someValue",
                [Guid.NewGuid().ToString("n")] = "someValue",
                [Guid.NewGuid().ToString("n")] = "someValue",
                [Guid.NewGuid().ToString("n")] = "someValue",
                [Guid.NewGuid().ToString("n")] = "someValue"
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
                        if (i % 50 == 0)
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
                if (test.IsFaulted)
                    await test;
                Xunit.Assert.True(test.IsCompletedSuccessfully);
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task QueryStreamingGetIds(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog { Owner = "users/3" }, "dogs/1");
                    session.Store(new Dog { Owner = "users/4" }, "dogs/2");
                    session.Store(new Dog { Owner = "users/5" }, "dogs/3");

                    session.Store(new User { Count = 7 }, "users/3");
                    session.Store(new User { Count = 19 }, "users/4");
                    session.Store(new User { Count = 13 }, "users/5");

                    session.SaveChanges();

                    if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                        Assert.NotEqual(await Sharding.GetShardNumberForAsync(store, "dogs/1"), await Sharding.GetShardNumberForAsync(store, "dogs/2"));

                    var q = session.Query<Dog>().Where(d => d.Id == "dogs/1" || d.Id == "dogs/2").OrderBy(x => x.Id);
                    var queryResult = session.Advanced.Stream<Dog>(q);

                    var resList = new List<Dog>();

                    foreach (var res in queryResult)
                    {
                        resList.Add(res.Document);
                    }

                    Assert.Equal(2, resList.Count);
                    Assert.Equal("dogs/1", resList[0].Id);
                    Assert.Equal("dogs/2", resList[1].Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task QueryStreamingLoadIds(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog { Owner = "users/3" }, "dogs/1");
                    session.Store(new Dog { Owner = "users/4" }, "dogs/2");
                    session.Store(new Dog { Owner = "users/5" }, "dogs/3");

                    session.Store(new User { Count = 7 }, "users/3");
                    session.Store(new User { Count = 19 }, "users/4");
                    session.Store(new User { Count = 13 }, "users/5");

                    session.SaveChanges();

                    if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                        Assert.NotEqual(await Sharding.GetShardNumberForAsync(store, "dogs/1"), await Sharding.GetShardNumberForAsync(store, "dogs/2"));

                    var q = session.Advanced.DocumentQuery<Dog>().WhereIn(x => x.Id, new[] { "dogs/1", "dogs/2" }).OrderBy("Id");
                    
                    var queryResult = session.Advanced.Stream<Dog>(q);

                    var resList = new List<Dog>();

                    foreach (var res in queryResult)
                    {
                        resList.Add(res.Document);
                    }

                    Assert.Equal(2, resList.Count);
                    Assert.Equal("dogs/1", resList[0].Id);
                    Assert.Equal("dogs/2", resList[1].Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Streaming_Results_Should_Sort_Properly(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                documentStore.ExecuteIndex(new FooIndex());

                using (var session = documentStore.OpenSession())
                {
                    var random = new System.Random();

                    for (int i = 0; i < 100; i++)
                        session.Store(new Foo { Num = random.Next(1, 100) });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);


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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Streaming_Query_Sort_By_Name(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    var random = new System.Random();

                    for (int i = 0; i < 100; i++)
                        session.Store(new User { Name = RandomString(random, 5) });

                    session.SaveChanges();
                }
                
                string last = null;

                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<User>().OrderBy(x => x.Name);

                    var enumerator = session.Advanced.Stream(q);
                    var count = 0;

                    while (enumerator.MoveNext())
                    {
                        string name = enumerator.Current.Document.Name;
                        
                        if (last != null)
                        {
                            // If the sort worked, this test should pass
                            Assert.True(string.Compare(last, name) <= 0);
                        }

                        last = name;
                        count++;
                    }
                    Assert.Equal(100, count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task Streaming_Query_Custom_Order_Fail(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var err = await Assert.ThrowsAsync<NotSupportedInShardingException>(async () =>
                    {
                        var q = session
                            .Advanced
                            .AsyncRawQuery<Company>("from Companies order by custom(Name, 'MySorter')");

                        var stream = await session.Advanced.StreamAsync(q);

                        while (await stream.MoveNextAsync())
                        {

                        }
                    });

                    Assert.Contains("Custom sorting is not supported in sharding as of yet", err.Message);
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
        
        private class Users_ByName_WithoutBoosting : AbstractIndexCreationTask<User>
        {
            public Users_ByName_WithoutBoosting()
            {
                Map = users => from u in users select new { Name = u.Name, LastName = u.LastName };

                Indexes.Add(x => x.Name, FieldIndexing.Search);

                IndexSuggestions.Add(x => x.Name);

                Analyzers.Add(x => x.Name, typeof(Lucene.Net.Analysis.SimpleAnalyzer).FullName);

                Stores.Add(x => x.Name, FieldStorage.Yes);
            }
        }

        private class Dog
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Owner { get; set; }
            public string Breed { get; set; }
            public string Color { get; set; }
            public int Age { get; set; }
            public bool IsVaccinated { get; set; }
        }

        private static string RandomString(Random random, int length)
        {
            const string pool = "abcdefghijklmnopqrstuvwxyz";
            var builder = new StringBuilder();

            for (var i = 0; i < length; i++)
            {
                var c = pool[random.Next(0, pool.Length)];
                builder.Append(c);
            }

            return builder.ToString();
        }
    }
}
