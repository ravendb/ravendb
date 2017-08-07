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

namespace SlowTests.Issues
{
    public class RavenDB_2672 : RavenTestBase
    {
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
                            Name = "Manufacturer"
                        },
                        new Facet
                        {
                            Name = "Cost_D_Range",
                            Mode = FacetMode.Ranges,
                            Ranges =
                            {
                                "[NULL TO 200.0]",
                                "[200.0 TO 400.0]",
                                "[400.0 TO 600.0]",
                                "[600.0 TO 800.0]",
                                "[800.0 TO NULL]"
                            }
                        },
                        new Facet
                        {
                            Name = "Megapixels_D_Range",
                            Mode = FacetMode.Ranges,
                            Ranges =
                            {
                                "[NULL TO 3.0]",
                                "[3.0 TO 7.0]",
                                "[7.0 TO 10.0]",
                                "[10.0 TO NULL]"
                            }
                        }
                     };

                    session.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    session.SaveChanges();

                    var e = Assert.Throws<InvalidQueryException>(() => session.Query<Camera>().Where(x => x.Cost >= 100 && x.Cost <= 300).ToFacets("facets/CameraFacets"));
                    Assert.Contains("Facet query must be executed against static index", e.Message);

                    var e2 = Assert.Throws<IndexDoesNotExistException>(() => session.Query<Camera>("SomeIndex").Where(x => x.Cost >= 100 && x.Cost <= 300).ToFacets("facets/CameraFacets"));
                    Assert.Contains("There is no index with 'SomeIndex' name.", e2.Message);
                }
            }
        }
    }
}
