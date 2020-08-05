using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
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

        private class Company_WithExtraField : Company
        {
            public string ExtraField { get; set; }
        }
    }
}
