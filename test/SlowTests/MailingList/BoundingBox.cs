// -----------------------------------------------------------------------
//  <copyright file="BoundingBox.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class BoundingBox : RavenTestBase
    {
        [Fact(Skip = "Missing feature: Spatial")]
        public void ShouldGetRightResults()
        {
            // verify using http://arthur-e.github.io/Wicket/sandbox-gmaps3.html
            var outer = "POLYGON((-2.14 53.0,-2.14 53.6,-1.52 53.6,-1.52 53.0,-2.14 53.0))";
            var inner = "POLYGON((-1.778 53.205,-1.778 53.207,-1.776 53.207,-1.776 53.205,-1.778 53.205))";

            using (var documentStore = GetDocumentStore())
            {
                new Shapes_SpatialIndex().Execute(documentStore);
                using (IDocumentSession db = documentStore.OpenSession())
                {
                    var shape = new Shape { Wkt = inner, };
                    db.Store(shape);
                    db.SaveChanges();
                }

                WaitForIndexing(documentStore);
                using (IDocumentSession db = documentStore.OpenSession())
                {
                    List<Shape> results = db.Query<Shape, Shapes_SpatialIndex>()
                                            .Customize(x => x.RelatesToShape("Bbox", outer, SpatialRelation.Within))
                                            .ToList(); // hangs

                    Assert.Equal(1, results.Count);
                }
            }
        }

        private class Shape
        {
            public int Id { get; set; }
            public string Wkt { get; set; }
        }

        private class Shapes_SpatialIndex : AbstractIndexCreationTask<Shape>
        {
            public Shapes_SpatialIndex()
            {
                Map = shapes => from s in shapes
                                select new
                                {
                                    __ = SpatialGenerate("Bbox", s.Wkt, SpatialSearchStrategy.GeohashPrefixTree, 6)
                                };
            }
        }
    }
}
