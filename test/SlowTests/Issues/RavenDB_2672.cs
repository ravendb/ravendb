// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2672.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2672 : RavenTestBase
    {
        public RavenDB_2672(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void FacetSearchShouldThrowIfIndexDoesNotExist()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var facets = new List<Facet>
                     {
                        new Facet
                        {
                            FieldName = "Manufacturer"
                        }
                     };

                    var ranges = new List<RangeFacet>
                    {
                        //default is term query		                         
                        //In Lucene [ is inclusive, { is exclusive
                        new RangeFacet()
                        {
                            Ranges =
                            {
                                "Cost <= 200",
                                "Cost BETWEEN 200 AND 400",
                                "Cost BETWEEN 400 AND 600",
                                "Cost BETWEEN 600 AND 800",
                                "Cost >= 800"
                            }
                        },
                        new RangeFacet
                        {
                            Ranges =
                            {
                                "Megapixels <= 3",
                                "Megapixels BETWEEN 3 AND 7",
                                "Megapixels BETWEEN 7 AND 10",
                                "Megapixels >= 10",
                            }
                        }
                    };

                    session.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets, RangeFacets = ranges });
                    session.SaveChanges();

                    var e = Assert.Throws<InvalidQueryException>(() => session.Query<Camera>().Where(x => x.Cost >= 100 && x.Cost <= 300).AggregateUsing("facets/CameraFacets").Execute());
                    Assert.Contains("Facet query must be executed against static index", e.Message);

                    var e2 = Assert.Throws<IndexDoesNotExistException>(() => session.Query<Camera>("SomeIndex").Where(x => x.Cost >= 100 && x.Cost <= 300).AggregateUsing("facets/CameraFacets").Execute());
                    Assert.Contains("There is no index with 'SomeIndex' name.", e2.Message);
                }
            }
        }
    }
}
