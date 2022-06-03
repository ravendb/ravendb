// -----------------------------------------------------------------------
//  <copyright file="RavenDB814.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class Q14235692 : RavenTestBase
    {
        public Q14235692(ITestOutputHelper output) : base(output)
        {
        }

        private class Company
        {
            public string Name { get; set; }
            public string Country { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Empty_Strings_Can_Be_Used_In_Where_Equals(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Company { Name = "A", Country = "USA" });
                    session.Store(new Company { Name = "B", Country = "" });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Company>()
                                         .Customize(x => x.WaitForNonStaleResults())
                                         .Where(c => c.Country == "")
                                         .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("B", results[0].Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Empty_Strings_Can_Be_Used_In_Where_In_Once(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Company { Name = "A", Country = "USA" });
                    session.Store(new Company { Name = "B", Country = "" });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Company>()
                                         .Customize(x => x.WaitForNonStaleResults())
                                         .Where(c => c.Country.In(new[] { "" }))
                                         .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("B", results[0].Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Empty_Strings_Can_Be_Used_In_Where_In_Twice(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Company { Name = "A", Country = "USA" });
                    session.Store(new Company { Name = "B", Country = "" });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Company>()
                                         .Customize(x => x.WaitForNonStaleResults())
                                         .Where(c => c.Country.In(new[] { "", "" }))
                                         .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("B", results[0].Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Empty_Strings_Can_Be_Used_In_Where_In_Thrice(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Company { Name = "A", Country = "USA" });
                    session.Store(new Company { Name = "B", Country = "" });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Company>()
                                         .Customize(x => x.WaitForNonStaleResults())
                                         .Where(c => c.Country.In(new[] { "", "", "" }))
                                         .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("B", results[0].Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Empty_Strings_Can_Be_Used_In_Where_In_With_Other_Data(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Company { Name = "A", Country = "USA" });
                    session.Store(new Company { Name = "B", Country = "" });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var results = session.Query<Company>()
                                         .Customize(x => x.WaitForNonStaleResults())
                                         .Where(c => c.Country.In(new[] { "USA", "" }))
                                         .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }
    }
}
