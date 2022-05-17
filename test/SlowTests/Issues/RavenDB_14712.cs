using Tests.Infrastructure;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;
using IndexingFields = Raven.Client.Constants.Documents.Indexing.Fields;


namespace SlowTests.Issues
{
    public class RavenDB_14712 : RavenTestBase
    {
        public RavenDB_14712(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void JavaScriptIndexesShouldNotIndexImplicitNullValues(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                JsIndex index = options.JavascriptEngineMode.ToString() == "Jint" ? new JsIndexJint() : new JsIndexV8();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", ExternalId = null }, "companies/1");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var termsCountNull = options.JavascriptEngineMode.ToString() == "Jint" ? 0 : 1;

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("true", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Equal(termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);

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
                Assert.Equal(1 + termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);
                Assert.Contains("torun", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("true", terms);
                Assert.Contains("false", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Equal(1 + termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);
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
                    var companies = (options.JavascriptEngineMode.ToString() == "Jint" ? session.Query<Company, JsIndexJint>() : session.Query<Company, JsIndexV8>())
                        .ToList();

                    Assert.Equal(2, companies.Count);
                    Assert.Contains("CF", companies.Select(x => x.Name));
                    Assert.Contains("HR", companies.Select(x => x.Name));
                }

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "City", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains(IndexingFields.NullValue, terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "IsNull", fromValue: null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("true", terms);
                Assert.Contains("false", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
                Assert.Equal(1 + termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);
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
                Assert.Contains(IndexingFields.NullValue, terms);

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
          
        }

        private class JsIndexJint : JsIndex
        {
            public JsIndexJint() 
            {
                var optChaining = "" ;

                Maps = new HashSet<string>
                {
                    @$"map('Companies', function (c) {{
                        var city = c.Address{optChaining}.City;
                        var isNull = false;
                        if (!city) {{
                            isNull = true;
                        }}

                        var product = load(c.ExternalId, 'Products');
                        var isProductNull = false;
                        if (!product) {{
                            isProductNull = true;
                        }}

                        return {{
                            City: c.Address{optChaining}.City,
                            IsNull: isNull,
                            Name: product{optChaining}.Name,
                            IsProductNull: isProductNull
                        }};
                    }})",
                };
            }
        }

        private class JsIndexV8 : JsIndex
        {
            public JsIndexV8()
            {
                var optChaining = "?";
                Maps = new HashSet<string>
                {
                    @$"map('Companies', function (c) {{
                        var city = c.Address{optChaining}.City;
                        var isNull = false;
                        if (!city) {{
                            isNull = true;
                        }}

                        var product = load(c.ExternalId, 'Products');
                        var isProductNull = false;
                        if (!product) {{
                            isProductNull = true;
                        }}

                        return {{
                            City: c.Address{optChaining}.City,
                            IsNull: isNull,
                            Name: product{optChaining}.Name,
                            IsProductNull: isProductNull
                        }};
                    }})",
                };
            }
        }
    }
}
