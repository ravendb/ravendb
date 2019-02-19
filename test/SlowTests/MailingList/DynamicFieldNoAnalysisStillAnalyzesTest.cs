using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

// ReSharper disable MemberCanBePrivate.Local
// ReSharper disable InconsistentNaming

namespace SlowTests.MailingList
{
    public class DynamicFieldNoAnalysisStillAnalyzesTest : RavenTestBase
    {

        [Fact]
        public void ToFacets_UsingDynamicFieldsWithoutAnalysis_ReturnsFacetValuesInOriginalCasing()
        {
            using (var _store = GetDocumentStore())
            using (var _session = _store.OpenSession())
            {
                new ItemsWithDynamicFieldsIndex().Execute(_store);
                var articleGroup = new Item
                {
                    Properties =
                                   {
                                       new Property
                                       {
                                           HeaderId = "brand",
                                           Values =
                                           {
                                               "Sony",
                                               "Samsung",
                                           },
                                       },
                                   },
                };

                _session.Store(articleGroup);
                _session.SaveChanges();

                WaitForIndexing(_store);

                var facets = _session.Advanced.DocumentQuery<Item, ItemsWithDynamicFieldsIndex>()
                    .AggregateBy(new[]
                    {
                        new Facet
                        {
                            FieldName = "prop_brand",
                        },
                    })
                    .Execute();

                Assert.True(facets.ContainsKey("prop_brand"));

                var facetValues = facets["prop_brand"].Values.Select(value => value.Range).ToArray();

                Assert.DoesNotContain("sony", facetValues, StringComparer.Ordinal);
                Assert.DoesNotContain("samsung", facetValues, StringComparer.Ordinal);
                Assert.Contains("Sony", facetValues, StringComparer.Ordinal);
                Assert.Contains("Samsung", facetValues, StringComparer.Ordinal);
            }
        }

        private sealed class Property
        {
            public Property()
            {
                Values = new List<string>();
            }

            public string HeaderId { get; set; }
            public List<string> Values { get; set; }
        }

        private sealed class Item
        {
            public Item()
            {
                Properties = new List<Property>();
            }

            public List<Property> Properties { get; set; }
        }

        private class ItemsWithDynamicFieldsIndex : AbstractIndexCreationTask<Item>
        {
            public ItemsWithDynamicFieldsIndex()
            {
                Map = items => from item in items
                               select new
                               {
                                   _ = item.Properties.Select(property => CreateField("prop_" + property.HeaderId,
                                                                                      property.Values,
                                                                                      true,
                                                                                      false)),
                               };
            }
        }
    }
}
