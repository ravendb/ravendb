using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

// ReSharper disable MemberCanBePrivate.Local
// ReSharper disable InconsistentNaming

namespace Raven.Tests.MailingList
{
    public class DynamicFieldNoAnalysisStillAnalyzesTest : RavenTestBase
    {

        [Fact]
        public void ToFacets_UsingDynamicFieldsWithoutAnalysis_ReturnsFacetValuesInOriginalCasing()
        {
            using (var _store = NewDocumentStore())
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
                                     .ToFacets(new[]
										   {
											   new Facet
											   {
												   Name = "prop_brand",
											   },
										   });

                Assert.True(facets.Results.ContainsKey("prop_brand"));

                var facetValues = facets.Results["prop_brand"].Values.Select(value => value.Range).ToArray();

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
