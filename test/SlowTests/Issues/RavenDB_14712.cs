using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14712 : RavenTestBase
    {
        public RavenDB_14712(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void JavaScriptIndexesShouldNotIndexImplicitNullValues()
        {
            using (var store = GetDocumentStore())
            {
                var index = new JsIndex();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Empty(terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("true", terms);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "CF", Address = new Address { City = "Torun" } }, "companies/2");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("true", terms);
                Assert.Contains("false", terms);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    company.Address = new Address { City = null };

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company, JsIndex>()
                        .ToList();

                    Assert.Equal(2, companies.Count);
                    Assert.Contains("CF", companies.Select(x => x.Name));
                    Assert.Contains("HR", companies.Select(x => x.Name));
                }

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("NULL_VALUE", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("true", terms);
                Assert.Contains("false", terms);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    company.Address = new Address { City = "Hadera" };

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("hadera", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("false", terms);
            }
        }

        private class JsIndex : AbstractJavaScriptIndexCreationTask
        {
            public JsIndex()
            {
                Maps = new HashSet<string>
                {
                    @"map('Companies', function (c) {
                        var city = c.Address.City;
                        var isNull = false;
                        if (!city) {
                            isNull = true;
                        }

                        return {
                            City: c.Address.City,
                            IsNull: isNull
                        };
                    })",
                };
            }
        }
    }
}
