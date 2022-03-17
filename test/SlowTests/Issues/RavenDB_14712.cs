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
                    session.Store(new Company { Name = "HR", ExternalId = null }, "companies/1");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Empty(terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("true", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Empty(terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsProductNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("true", terms);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "CF", ExternalId = "products/2", Address = new Address { City = "Torun" } }, "companies/2");
                    session.Store(new Product { Name = "P2" }, "products/2");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("true", terms);
                Assert.Contains("false", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("p2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsProductNull", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("true", terms);
                Assert.Contains("false", terms);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    company.Address = new Address { City = null };

                    session.Store(new Product { Name = null }, "products/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

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

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("p2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsProductNull", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("true", terms);
                Assert.Contains("false", terms);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    company.ExternalId = "products/1";
                    company.Address = new Address { City = "Hadera" };

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("hadera", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("false", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("p2", terms);
                Assert.Contains("NULL_VALUE", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsProductNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("false", terms);

                using (var session = store.OpenSession())
                {
                    var product = session.Load<Product>("products/1");
                    product.Name = "P1";

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("hadera", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("false", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("p1", terms);
                Assert.Contains("p2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsProductNull", fromValue: null));
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

                        var product = load(c.ExternalId, 'Products');
                        var isProductNull = false;
                        if (!product) {
                            isProductNull = true;
                        }

                        return {
                            City: c.Address.City,
                            IsNull: isNull,
                            Name: product.Name,
                            IsProductNull: isProductNull
                        };
                    })",
                };
            }
        }
    }
}
