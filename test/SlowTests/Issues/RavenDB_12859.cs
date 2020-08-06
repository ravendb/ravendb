using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12859 : RavenTestBase
    {
        public RavenDB_12859(ITestOutputHelper output) : base(output)
        {
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       Name = $"{company.Name}_Stored"
                                   };

                Store(x => x.Name, FieldStorage.Yes);
            }
        }

        private class Companies_ByName_Reduce : AbstractIndexCreationTask<Company, Companies_ByName_Reduce.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public int Count { get; set; }
            }

            public Companies_ByName_Reduce()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       Name = $"{company.Name}_Stored",
                                       Count = 1
                                   };

                Reduce = results => from result in results
                                    group result by result.Name into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };

                Store(x => x.Name, FieldStorage.Yes);
            }
        }

        private class Companies_ByName_TimeSeries : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public Companies_ByName_TimeSeries()
            {
                AddMap("HeartRate", segments => from segment in segments
                                                from entry in segment.Entries
                                                select new
                                                {
                                                    Count = 1
                                                });

                Store(x => x.Name, FieldStorage.Yes);
            }
        }

        private class Companies_ByName_Counters : AbstractCountersIndexCreationTask<Company>
        {
            public Companies_ByName_Counters()
            {
                AddMap("HeartRate", entries => from segment in entries
                                               select new
                                               {
                                                   Count = 1
                                               });

                Store(x => x.Name, FieldStorage.Yes);
            }
        }

        [Fact]
        public void Can_Use_Projection_Behavior()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", Fax = "123" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var name = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.Default);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromIndex);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromIndexOrThrow);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocument);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR", name);

                    name = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocumentOrThrow);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR", name);
                }

                using (var session = store.OpenSession())
                {
                    var fax = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                        })
                        .Select(x => x.Fax)
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax);

                    fax = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.Default);
                        })
                        .Select(x => x.Fax)
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax);

                    fax = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromIndex);
                        })
                        .Select(x => x.Fax)
                        .FirstOrDefault();

                    Assert.Null(fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromIndexOrThrow);
                        })
                        .Select(x => x.Fax)
                        .FirstOrDefault();
                    });

                    fax = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocument);
                        })
                        .Select(x => x.Fax)
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Query<Company_WithExtraField, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocumentOrThrow);
                        })
                        .Select(x => x.ExtraField)
                        .FirstOrDefault();
                    });
                }

                using (var session = store.OpenSession())
                {
                    var values = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                        })
                        .Select(x => new { x.Name, x.Fax })
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal("123", values.Fax);

                    values = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.Default);
                        })
                        .Select(x => new { x.Name, x.Fax })
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal("123", values.Fax);

                    values = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromIndex);
                        })
                        .Select(x => new { x.Name, x.Fax })
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal(null, values.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Query<Company, Companies_ByName>()
                            .Customize(x =>
                            {
                                x.NoCaching();
                                x.Projection(ProjectionBehavior.FromIndexOrThrow);
                            })
                            .Select(x => new { x.Name, x.Fax })
                            .FirstOrDefault();
                    });

                    values = session.Query<Company, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocument);
                        })
                        .Select(x => new { x.Name, x.Fax })
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR", values.Name);
                    Assert.Equal("123", values.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Query<Company_WithExtraField, Companies_ByName>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocumentOrThrow);
                        })
                        .Select(x => new { x.Name, x.ExtraField })
                        .FirstOrDefault();
                    });
                }
            }
        }

        [Fact]
        public void Using_Invalid_Projection_Behavior_Should_Throw()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName_Reduce().Execute(store);
                new Companies_ByName_Counters().Execute(store);
                new Companies_ByName_TimeSeries().Execute(store);

                using (var session = store.OpenSession())
                {
                    TestQuery<Companies_ByName_Reduce>(session);
                }

                using (var session = store.OpenSession())
                {
                    TestQuery<Companies_ByName_Counters>(session);
                }

                using (var session = store.OpenSession())
                {
                    TestQuery<Companies_ByName_TimeSeries>(session);
                }
            }

            static void TestQuery<TIndex>(IDocumentSession session)
                where TIndex : AbstractCommonApiForIndexes, new()
            {
                session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                    })
                    .Select(x => x.Name)
                    .FirstOrDefault();

                session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                        x.Projection(ProjectionBehavior.Default);
                    })
                    .Select(x => x.Name)
                    .FirstOrDefault();

                session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                        x.Projection(ProjectionBehavior.FromIndex);
                    })
                    .Select(x => x.Name)
                    .FirstOrDefault();

                session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                        x.Projection(ProjectionBehavior.FromIndexOrThrow);
                    })
                    .Select(x => x.Name)
                    .FirstOrDefault();

                Assert.Throws<InvalidQueryException>(() =>
                {
                    session.Query<ClassWithName, TIndex>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocument);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();
                });

                Assert.Throws<InvalidQueryException>(() =>
                {
                    session.Query<ClassWithName, TIndex>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocumentOrThrow);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefault();
                });
            }
        }

        private class Company_WithExtraField : Company
        {
            public string ExtraField { get; set; }
        }

        private class ClassWithName
        {
            public string Name { get; set; }
        }
    }
}
