// -----------------------------------------------------------------------
//  <copyright file="CustomAnalyzers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Highlighting;
using SlowTests.Core.Utils.Indexes;
using Xunit.Abstractions;

using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Indexing
{
    public class CustomAnalyzers : RavenTestBase
    {
        public CustomAnalyzers(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CreateAndQuerySimpleIndexWithSortingAndCustomCollateral()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_SortByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C" });
                    session.Store(new Company { Name = "a" });
                    session.Store(new Company { Name = "ć" });
                    session.Store(new Company { Name = "ą" });
                    session.Store(new Company { Name = "A" });
                    session.Store(new Company { Name = "c" });
                    session.Store(new Company { Name = "Ą" });
                    session.Store(new Company { Name = "D" });
                    session.Store(new Company { Name = "d" });
                    session.Store(new Company { Name = "b" });
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);
                    RavenTestHelper.AssertNoIndexErrors(store);

                    var companies = session.Query<Company, Companies_SortByName>()
                        .OrderBy(c => c.Name)
                        .ToArray();

                    Assert.Equal(10, companies.Length);
                    Assert.Equal("a", companies[0].Name);
                    Assert.Equal("A", companies[1].Name);
                    Assert.Equal("ą", companies[2].Name);
                    Assert.Equal("Ą", companies[3].Name);
                    Assert.Equal("b", companies[4].Name);
                    Assert.Equal("c", companies[5].Name);
                    Assert.Equal("C", companies[6].Name);
                    Assert.Equal("ć", companies[7].Name);
                    Assert.Equal("d", companies[8].Name);
                    Assert.Equal("D", companies[9].Name);
                }
            }
        }

        [Fact]
        public void CreateAndQuerySimpleIndexWithCustomAnalyzersAndFieldOptions()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_CustomAnalyzers().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 100;

                    session.Store(new Company
                    {
                        Name = "The lazy dogs, Bob@hotmail.com 123432.",
                        Desc = "The lazy dogs, Bob@hotmail.com 123432.",
                        Email = "test Bob@hotmail.com",
                        Address1 = "The lazy dogs, Bob@hotmail.com 123432.",
                        Address2 = "The lazy dogs, Bob@hotmail.com 123432.",
                        Address3 = "The lazy dogs, Bob@hotmail.com 123432.",
                        Phone = 111222333
                    }, "companies/1");
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    //StandardAnalyzer
                    var companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "bob")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "123432")
                        .ToArray();
                    Assert.Equal(1, companies.Length);

                    //StopAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "bob")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Search(c => c.Desc, "bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "123432")
                        .ToArray();
                    Assert.Equal(0, companies.Length);


                    //should not be analyzed
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Email == "bob")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Email == "test")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Email == "test Bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);

                    //SimpleAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "the")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "dogs")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Search(c => c.Address1, "the lazy dogs")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "bob")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "hotmail")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Search(c => c.Address1, "bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "123432")
                        .ToArray();
                    Assert.Equal(0, companies.Length);

                    //WhitespaceAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "dogs")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "dogs,")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "bob")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "hotmail")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "com")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Search(c => c.Address2, "Bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "123432")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "123432.")
                        .ToArray();
                    Assert.Equal(1, companies.Length);


                    //KeywordAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "123432.")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "lazy")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "dogs")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "Bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(0, companies.Length);

                    session.Store(new Company
                    {
                        Id = "companies/2",
                        Name = "The lazy dogs, Bob@hotmail.com lazy 123432 lazy dogs."
                    });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    companies = session.Advanced.DocumentQuery<Company>("Companies/CustomAnalyzers")
                        .Highlight("Name", 128, 1, out Highlightings highlightings)
                        .Search("Name", "lazy")
                        .ToArray();
                    Assert.Equal(2, companies.Length);

                    var expected = new Dictionary<string, string>
                    {
                        {
                            "companies/1",
                            "The <b style=\"background:yellow\">lazy</b> dogs, Bob@hotmail.com 123432."
                        },
                        {
                            "companies/2",
                            "The <b style=\"background:yellow\">lazy</b> dogs, Bob@hotmail.com <b style=\"background:yellow\">lazy</b> 123432 <b style=\"background:yellow\">lazy</b> dogs."
                        }
                    };

                    var fragments = highlightings.GetFragments(companies[0].Id);
                    Assert.Equal(expected[companies[0].Id], fragments.First());

                    fragments = highlightings.GetFragments(companies[1].Id);
                    Assert.Equal(expected[companies[1].Id], fragments.First());
                }
            }
        }
    }
}
