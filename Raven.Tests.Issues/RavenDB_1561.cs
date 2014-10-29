// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1561.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Common.Dto.Faceted;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1561 : FacetTestBase
    {

        private readonly IList<Camera> _data;
		private const int NumCameras = 100;

        public RavenDB_1561()
		{
			_data = GetCameras(NumCameras);
		}

        [Fact]
        public void CanPerformFacetedLimitSearchOnNonSystemDatabase()
        {
            var facets = new List<Facet> { new Facet { Name = "Manufacturer", MaxResults = 2, TermSortMode = FacetTermSortMode.HitsAsc, IncludeRemainingTerms = true } };

            using (var store = NewRemoteDocumentStore(databaseName: "test")) 
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    s.SaveChanges();

                    var facetResults = s.Query<Camera>("CameraCost")
                        .ToFacets("facets/CameraFacets");

                    Assert.NotNull(facetResults);
                }
            }
        }

        private void Setup(IDocumentStore store)
        {
            using (var s = store.OpenSession())
            {
                store.DatabaseCommands.PutIndex("CameraCost",
                                                new IndexDefinition
                                                {
                                                    Map =
                                                        @"from camera in docs 
                                                        select new 
                                                        { 
                                                            camera.Manufacturer, 
                                                            camera.Model, 
                                                            camera.Cost,
                                                            camera.DateOfListing,
                                                            camera.Megapixels
                                                        }"
                                                });

                var counter = 0;
                foreach (var camera in _data)
                {
                    s.Store(camera);
                    counter++;

                    if (counter % (NumCameras / 25) == 0)
                        s.SaveChanges();
                }
                s.SaveChanges();

                s.Query<Camera>("CameraCost")
                    .Customize(x => x.WaitForNonStaleResults())
                    .ToList();
            }
        }
    }
}