// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using Xunit;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Client.Linq.Indexing;
using Raven.Client.Operations.Databases.Indexes;
using Sparrow.Json;

namespace NewClientTests.NewClient.Raven.Tests.Core.Stream
{
    public class QueryStreaming : RavenNewTestBase
    {
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
                    JsonOperationContext context;
                    store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);

                    var indexDef = new IndexDefinitionBuilder<MyClass>()
                    {
                        Map = docs => from doc in docs select new { Index = doc.Index }
                    };

                    store.Admin.Send(new PutIndexOperation("MyClass/ByIndex", indexDef.ToIndexDefinition(store.Conventions, true)));

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

                Sort(x => x.Num, SortOptions.NumericDefault);
            }
        }
    }

    public class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { Name = u.Name, LastName = u.LastName.Boost(10) };

            Indexes.Add(x => x.Name, FieldIndexing.Analyzed);

            IndexSuggestions.Add(x => x.Name);

            Analyzers.Add(x => x.Name, typeof(Lucene.Net.Analysis.SimpleAnalyzer).FullName);

            Stores.Add(x => x.Name, FieldStorage.Yes);
        }
    }


}
