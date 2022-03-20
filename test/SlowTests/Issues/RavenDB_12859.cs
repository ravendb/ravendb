using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
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
        public void Can_Use_Projection_Behavior_Query()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", Fax = "123" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

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
        public void Can_Use_Projection_Behavior_Query_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", Fax = "123" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var name = session.Advanced
                        .RawQuery<ClassWithName>("from index 'Companies/ByName' as c select { Name : c.Name }")
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name.Name);

                    name = session.Advanced
                        .RawQuery<ClassWithName>("from index 'Companies/ByName' as c select { Name : c.Name }")
                        .Projection(ProjectionBehavior.Default)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name.Name);

                    name = session.Advanced
                        .RawQuery<ClassWithName>("from index 'Companies/ByName' as c select { Name : c.Name }")
                        .Projection(ProjectionBehavior.FromIndex)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name.Name);

                    name = session.Advanced
                        .RawQuery<ClassWithName>("from index 'Companies/ByName' as c select { Name : c.Name }")
                        .Projection(ProjectionBehavior.FromIndex)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name.Name);

                    name = session.Advanced
                        .RawQuery<ClassWithName>("from index 'Companies/ByName' as c select { Name : c.Name }")
                        .Projection(ProjectionBehavior.FromDocument)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR", name.Name);

                    name = session.Advanced
                        .RawQuery<ClassWithName>("from index 'Companies/ByName' as c select { Name : c.Name }")
                        .Projection(ProjectionBehavior.FromDocumentOrThrow)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR", name.Name);
                }

                using (var session = store.OpenSession())
                {
                    var fax = session.Advanced
                        .RawQuery<ClassWithFax>("from index 'Companies/ByName' as c select { Fax : c.Fax }")
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax.Fax);

                    fax = session.Advanced
                        .RawQuery<ClassWithFax>("from index 'Companies/ByName' as c select { Fax : c.Fax }")
                        .Projection(ProjectionBehavior.Default)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax.Fax);

                    fax = session.Advanced
                        .RawQuery<ClassWithFax>("from index 'Companies/ByName' as c select { Fax : c.Fax }")
                        .Projection(ProjectionBehavior.FromIndex)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.Null(fax.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced
                            .RawQuery<ClassWithFax>("from index 'Companies/ByName' as c select { Fax : c.Fax }")
                            .Projection(ProjectionBehavior.FromIndexOrThrow)
                            .NoCaching()
                            .FirstOrDefault();
                    });

                    fax = session.Advanced
                        .RawQuery<ClassWithFax>("from index 'Companies/ByName' as c select { Fax : c.Fax }")
                        .Projection(ProjectionBehavior.FromDocument)
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced
                            .RawQuery<Company_WithExtraField>("from index 'Companies/ByName' as c select { ExtraField : c.ExtraField }")
                            .Projection(ProjectionBehavior.FromDocumentOrThrow)
                            .NoCaching()
                            .FirstOrDefault();
                    });
                }

                using (var session = store.OpenSession())
                {
                    var values = session.Advanced
                        .RawQuery<ClassWithNameAndFax>("from index 'Companies/ByName' as c select { Name : c.Name, Fax: c.Fax }")
                        .NoCaching()
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal("123", values.Fax);

                    values = session.Advanced
                        .RawQuery<ClassWithNameAndFax>("from index 'Companies/ByName' as c select { Name : c.Name, Fax: c.Fax }")
                        .NoCaching()
                        .Projection(ProjectionBehavior.Default)
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal("123", values.Fax);

                    values = session.Advanced
                        .RawQuery<ClassWithNameAndFax>("from index 'Companies/ByName' as c select { Name : c.Name, Fax: c.Fax }")
                        .NoCaching()
                        .Projection(ProjectionBehavior.FromIndex)
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal(null, values.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        values = session.Advanced
                            .RawQuery<ClassWithNameAndFax>("from index 'Companies/ByName' as c select { Name : c.Name, Fax: c.Fax }")
                            .NoCaching()
                            .Projection(ProjectionBehavior.FromIndexOrThrow)
                            .FirstOrDefault();
                    });

                    values = session.Advanced
                            .RawQuery<ClassWithNameAndFax>("from index 'Companies/ByName' as c select { Name : c.Name, Fax: c.Fax }")
                            .NoCaching()
                            .Projection(ProjectionBehavior.FromDocument)
                            .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR", values.Name);
                    Assert.Equal("123", values.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced
                            .RawQuery<Company_WithExtraField>("from index 'Companies/ByName' as c select { Name : c.Name, ExtraField: c.ExtraField }")
                            .NoCaching()
                            .Projection(ProjectionBehavior.FromDocumentOrThrow)
                            .FirstOrDefault();
                    });
                }
            }
        }

        [Fact]
        public void Can_Use_Projection_Behavior_DocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", Fax = "123" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var name = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>("Name")
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.Default, "Name")
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromIndex, "Name")
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromIndexOrThrow, "Name")
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR_Stored", name);

                    name = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromDocument, "Name")
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR", name);

                    name = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromDocumentOrThrow, "Name")
                        .FirstOrDefault();

                    Assert.NotNull(name);
                    Assert.Equal("HR", name);
                }

                using (var session = store.OpenSession())
                {
                    var fax = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>("Fax")
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax);

                    fax = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.Default, "Fax")
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax);

                    fax = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromIndex, "Fax")
                        .FirstOrDefault();

                    Assert.Null(fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromIndexOrThrow, "Fax")
                        .FirstOrDefault();
                    });

                    fax = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromDocument, "Fax")
                        .FirstOrDefault();

                    Assert.NotNull(fax);
                    Assert.Equal("123", fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced.DocumentQuery<Company_WithExtraField, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<string>(ProjectionBehavior.FromDocumentOrThrow, "ExtraField")
                        .FirstOrDefault();
                    });
                }

                using (var session = store.OpenSession())
                {
                    var values = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<ClassWithNameAndFax>("Name", "Fax")
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal("123", values.Fax);

                    values = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<ClassWithNameAndFax>(ProjectionBehavior.Default, "Name", "Fax")
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal("123", values.Fax);

                    values = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<ClassWithNameAndFax>(ProjectionBehavior.FromIndex, "Name", "Fax")
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR_Stored", values.Name);
                    Assert.Equal(null, values.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<ClassWithNameAndFax>(ProjectionBehavior.FromIndexOrThrow, "Name", "Fax")
                            .FirstOrDefault();
                    });

                    values = session.Advanced.DocumentQuery<Company, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<ClassWithNameAndFax>(ProjectionBehavior.FromDocument, "Name", "Fax")
                        .FirstOrDefault();

                    Assert.NotNull(values);
                    Assert.Equal("HR", values.Name);
                    Assert.Equal("123", values.Fax);

                    Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced.DocumentQuery<Company_WithExtraField, Companies_ByName>()
                        .NoCaching()
                        .SelectFields<ClassWithNameAndExtraField>(ProjectionBehavior.FromDocumentOrThrow, "Name", "ExtraField")
                        .FirstOrDefault();
                    });
                }
            }
        }

        [Fact]
        public async Task Using_Invalid_Projection_Behavior_Should_Throw()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName_Reduce().Execute(store);
                new Companies_ByName_Counters().Execute(store);
                new Companies_ByName_TimeSeries().Execute(store);

                using (var session = store.OpenSession())
                {
                    TestQuery<Companies_ByName_Reduce>(session);
                    TestQuery<Companies_ByName_Counters>(session);
                    TestQuery<Companies_ByName_TimeSeries>(session);
                }

                using (var session = store.OpenSession())
                {
                    TestDocumentQuery<Companies_ByName_Reduce>(session);
                    TestDocumentQuery<Companies_ByName_Counters>(session);
                    TestDocumentQuery<Companies_ByName_TimeSeries>(session);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await TestQueryAsync<Companies_ByName_Reduce>(session);
                    await TestQueryAsync<Companies_ByName_Counters>(session);
                    await TestQueryAsync<Companies_ByName_TimeSeries>(session);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await TestDocumentQueryAsync<Companies_ByName_Reduce>(session);
                    await TestDocumentQueryAsync<Companies_ByName_Counters>(session);
                    await TestDocumentQueryAsync<Companies_ByName_TimeSeries>(session);
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

            static void TestDocumentQuery<TIndex>(IDocumentSession session)
                where TIndex : AbstractCommonApiForIndexes, new()
            {
                session.Advanced.DocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>("Name")
                    .FirstOrDefault();

                session.Advanced.DocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>(ProjectionBehavior.Default, "Name")
                    .FirstOrDefault();

                session.Advanced.DocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>(ProjectionBehavior.FromIndex, "Name")
                    .FirstOrDefault();

                session.Advanced.DocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>(ProjectionBehavior.FromIndexOrThrow, "Name")
                    .FirstOrDefault();

                Assert.Throws<InvalidQueryException>(() =>
                {
                    session.Advanced.DocumentQuery<ClassWithName, TIndex>()
                        .NoCaching()
                        .SelectFields<ClassWithName>(ProjectionBehavior.FromDocument, "Name")
                        .FirstOrDefault();
                });

                Assert.Throws<InvalidQueryException>(() =>
                {
                    session.Advanced.DocumentQuery<ClassWithName, TIndex>()
                        .NoCaching()
                        .SelectFields<ClassWithName>(ProjectionBehavior.FromDocumentOrThrow, "Name")
                        .FirstOrDefault();
                });
            }

            static async Task TestQueryAsync<TIndex>(IAsyncDocumentSession session)
                where TIndex : AbstractCommonApiForIndexes, new()
            {
                await session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                    })
                    .Select(x => x.Name)
                    .FirstOrDefaultAsync();

                await session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                        x.Projection(ProjectionBehavior.Default);
                    })
                    .Select(x => x.Name)
                    .FirstOrDefaultAsync();

                await session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                        x.Projection(ProjectionBehavior.FromIndex);
                    })
                    .Select(x => x.Name)
                    .FirstOrDefaultAsync();

                await session.Query<ClassWithName, TIndex>()
                    .Customize(x =>
                    {
                        x.NoCaching();
                        x.Projection(ProjectionBehavior.FromIndexOrThrow);
                    })
                    .Select(x => x.Name)
                    .FirstOrDefaultAsync();

                await Assert.ThrowsAsync<InvalidQueryException>(async () =>
                {
                    await session.Query<ClassWithName, TIndex>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocument);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefaultAsync();
                });

                await Assert.ThrowsAsync<InvalidQueryException>(async () =>
                {
                    await session.Query<ClassWithName, TIndex>()
                        .Customize(x =>
                        {
                            x.NoCaching();
                            x.Projection(ProjectionBehavior.FromDocumentOrThrow);
                        })
                        .Select(x => x.Name)
                        .FirstOrDefaultAsync();
                });
            }

            static async Task TestDocumentQueryAsync<TIndex>(IAsyncDocumentSession session)
                where TIndex : AbstractCommonApiForIndexes, new()
            {
                await session.Advanced.AsyncDocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>("Name")
                    .FirstOrDefaultAsync();

                await session.Advanced.AsyncDocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>(ProjectionBehavior.Default, "Name")
                    .FirstOrDefaultAsync();

                await session.Advanced.AsyncDocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>(ProjectionBehavior.FromIndex, "Name")
                    .FirstOrDefaultAsync();

                await session.Advanced.AsyncDocumentQuery<ClassWithName, TIndex>()
                    .NoCaching()
                    .SelectFields<ClassWithName>(ProjectionBehavior.FromIndexOrThrow, "Name")
                    .FirstOrDefaultAsync();

                await Assert.ThrowsAsync<InvalidQueryException>(async () =>
                {
                    await session.Advanced.AsyncDocumentQuery<ClassWithName, TIndex>()
                        .NoCaching()
                        .SelectFields<ClassWithName>(ProjectionBehavior.FromDocument, "Name")
                        .FirstOrDefaultAsync();
                });

                await Assert.ThrowsAsync<InvalidQueryException>(async () =>
                {
                    await session.Advanced.AsyncDocumentQuery<ClassWithName, TIndex>()
                        .NoCaching()
                        .SelectFields<ClassWithName>(ProjectionBehavior.FromDocumentOrThrow, "Name")
                        .FirstOrDefaultAsync();
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

        private class ClassWithFax
        {
            public string Fax { get; set; }
        }

        private class ClassWithNameAndFax : ClassWithName
        {
            public string Fax { get; set; }
        }

        private class ClassWithNameAndExtraField : ClassWithName
        {
            public string ExtraField { get; set; }
        }
    }
}
