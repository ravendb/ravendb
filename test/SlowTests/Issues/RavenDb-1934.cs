using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDb_1934 : RavenTestBase
    {
        public RavenDb_1934(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Id { get; set; }
            public int? Age { get; set; }
            public short? TestShort { get; set; }
            public float? Grade { get; set; }
            public double? Price { get; set; }
            public string Name { get; set; }
            public TimeSpan? Start { get; set; }
            public TimeSpan Until { get; set; }
        }

        private class Bar
        {
            public string Id { get; set; }
            public string SomeData { get; set; }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Under_A_Day()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(20) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(15);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Over_A_Day()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(30), Until = TimeSpan.FromHours(40) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(35);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Mixed_Days()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(20), Until = TimeSpan.FromHours(30) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(25);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_VeryLarge()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromDays(100) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromDays(2);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Get_Range_Negatives()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(-10), Until = TimeSpan.FromHours(10) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var time = TimeSpan.FromHours(1);
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .SingleOrDefault(x => x.Start <= time && x.Until > time);

                    Assert.NotNull(result);
                }
            }
        }

        [Fact]
        public void Can_Sort_On_TimeSpans()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Start = TimeSpan.FromSeconds(10) });
                    session.Store(new Foo { Id = "2", Start = TimeSpan.FromSeconds(20) });
                    session.Store(new Foo { Id = "3", Start = TimeSpan.FromSeconds(15) });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .OrderBy(x => x.Start)
                        .ToArray();

                    Assert.Equal("1", results[0].Id);
                    Assert.Equal("3", results[1].Id);
                    Assert.Equal("2", results[2].Id);
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .OrderByDescending(x => x.Start)
                        .ToArray();

                    Assert.Equal("1", results[2].Id);
                    Assert.Equal("3", results[1].Id);
                    Assert.Equal("2", results[0].Id);
                }
            }
        }

        [Fact]
        public void Can_Sort_On_TimeSpans_With_Nulls()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Start = TimeSpan.FromSeconds(10) });
                    session.Store(new Foo { Id = "2", Start = TimeSpan.FromSeconds(20) });
                    session.Store(new Foo { Id = "3", Start = TimeSpan.FromSeconds(15) });
                    session.Store(new Foo { Id = "4", Start = null });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {

                    var results = session.Query<Foo>()
                           .OrderBy(x => x.Start)
                           .ToArray();


                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }
            }
        }


        [Fact]
        public void Can_Sort_On_Ints_With_Nulls()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Age = 10 });
                    session.Store(new Foo { Id = "2", Age = 20 });
                    session.Store(new Foo { Id = "3", Age = 15 });
                    session.Store(new Foo { Id = "4", Age = null });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Age)
                        .ToArray();


                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }


            }
        }
        [Fact]
        public void Can_Sort_On_Short_With_Nulls()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", TestShort = 10 });
                    session.Store(new Foo { Id = "2", TestShort = 20 });
                    session.Store(new Foo { Id = "3", TestShort = 15 });
                    session.Store(new Foo { Id = "4", TestShort = null });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.TestShort)
                        .ToArray();


                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }
            }
        }
        [Fact]
        public void Can_Sort_On_Float_With_Nulls()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Grade = (float?)10.1 });
                    session.Store(new Foo { Id = "2", Grade = (float?)20.4 });
                    session.Store(new Foo { Id = "3", Grade = (float?)15.5 });
                    session.Store(new Foo { Id = "4", Grade = null });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Grade)
                        .ToArray();


                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }


            }
        }

        [Fact]
        public void Can_Sort_On_Double_With_Nulls()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Price = 10.1 });
                    session.Store(new Foo { Id = "2", Price = 20.4 });
                    session.Store(new Foo { Id = "3", Price = 15.5 });
                    session.Store(new Foo { Id = "4", Price = null });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Price)
                        .ToArray();


                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }


            }
        }

        [Fact]
        public void Can_Sort_On_String_With_Nulls()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Name = "aaa" });
                    session.Store(new Foo { Id = "2", Name = "cc" });
                    session.Store(new Foo { Id = "3", Name = "b1" });
                    session.Store(new Foo { Id = "4", Name = null });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Name)
                        .ToArray();


                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }


            }
        }

        [Fact]
        public void Can_Sort_On_TimeSpans_With_Nulls_Using_MultiMap_Idx()
        {
            using (var documentStore = GetDocumentStore())
            {
                new TimeSpanTestMultiMapIndex().Execute(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Id = "1", Start = TimeSpan.FromSeconds(10) });
                    session.Store(new Foo { Id = "2", Start = TimeSpan.FromSeconds(20) });
                    session.Store(new Foo { Id = "3", Start = TimeSpan.FromSeconds(15) });
                    session.Store(new Bar { Id = "4" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo, TimeSpanTestMultiMapIndex>()
                        .OrderBy(x => x.Start)
                        .ProjectInto<Foo>()
                        .ToArray();

                    Assert.Equal("4", results[0].Id);
                    Assert.Equal("1", results[1].Id);
                    Assert.Equal("3", results[2].Id);
                    Assert.Equal("2", results[3].Id);
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Foo, TimeSpanTestMultiMapIndex>()
                        .OrderByDescending(x => x.Start)
                        .ProjectInto<Foo>()
                        .ToArray();

                    Assert.Equal("4", results[3].Id);
                    Assert.Equal("1", results[2].Id);
                    Assert.Equal("3", results[1].Id);
                    Assert.Equal("2", results[0].Id);
                }
            }
        }

        private class TimeSpanTestMultiMapIndex : AbstractMultiMapIndexCreationTask<Foo>
        {
            public TimeSpanTestMultiMapIndex()
            {
                AddMap<Foo>(docs => from d in docs
                                    select new Foo
                                    {
                                        Start = d.Start,
                                    });

                AddMap<Bar>(docs => from d in docs
                                    select new Foo
                                    {
                                        Start = null,
                                    });
            }
        }
    }
}

