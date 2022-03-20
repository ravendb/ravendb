// -----------------------------------------------------------------------
//  <copyright file="BoundingBox.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class BoundingBox : RavenTestBase
    {
        public BoundingBox(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
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

                Indexes.WaitForIndexing(documentStore);
                using (IDocumentSession db = documentStore.OpenSession())
                {
                    List<Shape> results = db.Query<Shapes_SpatialIndex.Result, Shapes_SpatialIndex>()
                        .Spatial(x => x.Bbox, x => x.RelatesToShape(outer, SpatialRelation.Within))
                        .OfType<Shape>()
                        .ToList(); // hangs

                    Assert.Equal(1, results.Count);
                }
            }
        }

        private class Shape
        {
            public string Id { get; set; }
            public string Wkt { get; set; }
        }

        private class Shapes_SpatialIndex : AbstractIndexCreationTask<Shape>
        {
            public class Result
            {
                public string Bbox { get; set; }
            }

            public Shapes_SpatialIndex()
            {
                Map = shapes => from s in shapes
                                select new
                                {
                                    Bbox = CreateSpatialField(s.Wkt)
                                };

                SpatialIndexesStrings.Add("Bbox", new SpatialOptions
                {
                    MaxTreeLevel = 6,
                    Strategy = SpatialSearchStrategy.GeohashPrefixTree
                });
            }
        }
    }
}
