using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class SkippedResults : RavenTestBase
    {
        [Fact]
        public void Can_page_when_using_nested_property_index()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i < 11; i++)
                    {
                        session.Store(CreateProvider(i));
                    }
                    session.SaveChanges();
                }

                new NestedPropertyIndex1().Execute(store);

                WaitForIndexing(store);

                // Retrieves all 10 providers in a single result set.
                using (var session = store.OpenSession())
                {
                    var result = (from p in session.Query<Provider, NestedPropertyIndex1>()
                                  .Customize(x => x.WaitForNonStaleResults())
                                  where p.Zip == "97520"
                                  select p).Take(1024).ToArray();
                    Assert.Equal(10, result.Count());
                }

                // Retrieves all 10 providers, 2 at a time.
                using (var session = store.OpenSession())
                {
                    const int pageSize = 2;
                    int pageNumber = 0;
                    int skippedResults = 0;
                    int recordsToSkip = 0;

                    var providers = new List<Provider>();

                    QueryStatistics statistics;
                    while (true)
                    {
                        var result = (from p in session.Query<Provider, NestedPropertyIndex1>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .Statistics(out statistics)
                                      where p.Zip == "97520"
                                      select p)
                            .Take(pageSize)
                            .Skip(recordsToSkip)
                            .ToList();

                        providers.AddRange(result);

                        if (result.Count < pageSize)
                            break;

                        pageNumber++;

                        skippedResults += statistics.SkippedResults;
                        recordsToSkip = pageSize * pageNumber + skippedResults;

                        // I found this in the Raven.Tests.MailingList.Vlad.WillOnlyGetPost2Once() method
                        //recordsToSkip = pageSize * pageNumber + statistics.SkippedResults;
                    }

                    Assert.Equal(10, providers.Count);
                    Assert.Equal(5, pageNumber);
                }
            }
        }

        private Provider CreateProvider(int i)
        {
            return new Provider
            {
                Name = "Test " + i,
                Zip = "97520",
                Categories = new List<Category>
                {
                    new Category
                    {
                        EffectiveFrom = DateTime.Now,
                        EffectiveThrough = DateTime.Now,
                        Name = "a"
                    },
                    new Category
                    {
                        EffectiveFrom = DateTime.Now,
                        EffectiveThrough = DateTime.Now,
                        Name = "a"
                    },
                    new Category
                    {
                        EffectiveFrom = DateTime.Now,
                        EffectiveThrough = DateTime.Now,
                        Name = "a"
                    },
                }
            };
        }


        private class Category
        {
            public string Name { get; set; }
            public DateTime EffectiveFrom { get; set; }
            public DateTime EffectiveThrough { get; set; }
        }


        private class Provider
        {
            public string Name { get; set; }
            public string Zip { get; set; }
            public IList<Category> Categories { get; set; }
        }


        // Indexing nested properties
        // Creates multiple index entries, one for each nested property combination
        private class NestedPropertyIndex1 : AbstractIndexCreationTask<Provider>
        {
            public NestedPropertyIndex1()
            {
                Map = providers =>
                      from p in providers
                      from c in p.Categories
                      select new
                      {
                          p.Name,
                          p.Zip,
                          Categories_Name = c.Name,
                          Categories_EffectiveFrom = c.EffectiveFrom,
                          Categories_EffectiveThrough = c.EffectiveThrough,
                      };
            }

        }
    }
}
