// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1411.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_1411 : RavenTestBase
    {
        public RavenDB_1411(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Id { get; set; }
            public string Item { get; set; }
        }

        private class Bar
        {
            public string Id { get; set; }
            public string Item { get; set; }
        }

        private class Baz
        {
            public string Id { get; set; }
            public string Item { get; set; }
        }

        private class SingleMapIndex : AbstractIndexCreationTask<Foo>
        {
            public SingleMapIndex()
            {
                Map = foos => from foo in foos select new { foo.Item };
            }
        }

        private class MultiMapIndex : AbstractMultiMapIndexCreationTask<MultiMapOutput>
        {
            public MultiMapIndex()
            {
                AddMap<Foo>(foos => from foo in foos select new { foo.Item });
                AddMap<Bar>(bars => from bar in bars select new { bar.Item });
            }
        }

        private class FooMapReduceIndex : AbstractIndexCreationTask<Foo, FooMapReduceIndex.Result>
        {
            public class Result
            {
                public string Item { get; set; }
                public int Count { get; set; }
            }

            public FooMapReduceIndex()
            {
                Map = foos => from f in foos select new { f.Item, Count = 1 };
                Reduce =
                    results =>
                    from result in results
                    group result by result.Item
                    into g
                    select new { Item = g.Key, Count = g.Sum(x => x.Count) };
            }
        }

        private class MultiMapOutput
        {
            public string Item { get; set; }
        }

        [Fact]
        public void OptimizationShouldWork_NewIndexedWillGetPrecomputedDocumentsToIndexToAvoidRetrievingAllDocumentsFromDisk()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Foo
                        {
                            Item = "Ball/" + i % 2
                        });

                        session.Store(new Bar
                        {
                            Item = "Computer/" + i
                        });
                    }

                    for (int i = 0; i < 10000; i++)
                    {
                        session.Store(new Baz
                        {
                            Item = "Baz/" + i
                        });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                new SingleMapIndex().Execute(store);
                new MultiMapIndex().Execute(store);
                new FooMapReduceIndex().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var count1 = session.Query<Foo, SingleMapIndex>().Count();
                    Assert.Equal(10, count1);

                    var count2 = session.Query<MultiMapOutput, MultiMapIndex>().Count();
                    Assert.Equal(20, count2);

                    var count3 = session.Query<FooMapReduceIndex.Result, FooMapReduceIndex>().ToList();
                    Assert.Equal(2, count3.Count);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Foo
                    {
                        Item = "Ball/" + 999
                    });

                    session.SaveChanges();

                    var count1 = session.Query<Foo, SingleMapIndex>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(11, count1);

                    var count2 =
                        session.Query<MultiMapOutput, MultiMapIndex>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(21, count2);

                    var count3 = session.Query<FooMapReduceIndex.Result, FooMapReduceIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                    Assert.Equal(3, count3.Count);
                }
            }
        }

        [Fact]
        public void NewIndexesForWhichOptimizationIsNotAppliedShouldBeProcessesCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        session.Store(new Foo
                        {
                            Item = "Foo/" + i
                        });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                new SingleMapIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Foo, SingleMapIndex>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(10000, count);
                }
            }
        }

        [Fact]
        public void ShouldGetAllNecessaryDocumentsToIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 800; i++)
                    {
                        session.Store(new Bar
                        {
                            Item = "Bar/" + i
                        });
                    }

                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new Foo
                        {
                            Item = "Foo/" + i
                        });
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                new SingleMapIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Foo, SingleMapIndex>().Customize(x => x.WaitForNonStaleResults()).Count();
                    Assert.Equal(200, count);
                }
            }
        }
    }
}
