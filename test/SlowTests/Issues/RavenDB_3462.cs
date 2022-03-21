// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3462.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3462 : RavenTestBase
    {
        public RavenDB_3462(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void BoundingBoxIndexSparialSearch()
        {
            using (var documentStore = GetDocumentStore())
            {
                new EntitySpatialIndex().Execute(documentStore);
                new EntitySpatialIndex2().Execute(documentStore);

                var entity = new Entity
                {
                    Geolocation = new Geolocation() { Lon = 12.559509, Lat = 55.673981 }, // POINT(12.559509 55.673981)
                    Id = "fooid"
                };

                using (var session = documentStore.OpenSession())
                {
                    session.Store(entity);
                    session.SaveChanges();

                    Indexes.WaitForIndexing(documentStore);

                    //Point(12.556675672531128 55.675285554217), corner of the bounding rectangle below
                    var nearbyPoints = session.Query<EntitySpatialIndex.Result, EntitySpatialIndex>()
                        .Spatial(x => x.Coordinates, x => x.WithinRadius(1, 55.675285554217, 12.556675672531128))
                        .OfType<Entity>()
                        .ToList();

                    Assert.Equal(1, nearbyPoints.Count); // Passes

                    nearbyPoints = session.Query<EntitySpatialIndex2.Result, EntitySpatialIndex2>()
                        .Spatial(x => x.Coordinates, x => x.WithinRadius(1, 55.675285554217, 12.556675672531128))
                        .OfType<Entity>()
                        .ToList();

                    Assert.Equal(1, nearbyPoints.Count);

                    var boundingRectangleWKT =
                        "POLYGON((12.556675672531128 55.675285554217,12.56213665008545 55.675285554217,12.56213665008545 55.67261750095371,12.556675672531128 55.67261750095371,12.556675672531128 55.675285554217))";

                    var q = session.Query<EntitySpatialIndex.Result, EntitySpatialIndex>()
                        .Spatial(x => x.Coordinates, x => x.RelatesToShape(boundingRectangleWKT, SpatialRelation.Within))
                        .OfType<Entity>()
                        .ToList();

                    Assert.Equal(1, q.Count);

                    q = session.Query<EntitySpatialIndex2.Result, EntitySpatialIndex2>()
                        .Spatial(x => x.Coordinates, x => x.RelatesToShape(boundingRectangleWKT, SpatialRelation.Within))
                        .OfType<Entity>()
                        .ToList();

                    Assert.Equal(1, q.Count); // does not pass
                }
            }
        }

        private class Entity
        {
            public string Id { get; set; }
            public Geolocation Geolocation { get; set; }
        }

        private class Geolocation
        {
            public double Lon { get; set; }
            public double Lat { get; set; }
            public string WKT
            {
                get
                {
                    return string.Format("POINT({0} {1})",
                        Lon.ToString(CultureInfo.InvariantCulture),
                        Lat.ToString(CultureInfo.InvariantCulture));
                }
            }
        }

        private class EntitySpatialIndex : AbstractIndexCreationTask<Entity>
        {
            public class Result
            {
                public string Coordinates { get; set; }
            }

            public EntitySpatialIndex()
            {
                Map = entities => entities.Select(entity => new
                {
                    entity.Id,
                    Coordinates = CreateSpatialField(entity.Geolocation.WKT)
                });

                Spatial("Coordinates", x => x.Cartesian.BoundingBoxIndex());
            }
        }

        private class EntitySpatialIndex2 : AbstractIndexCreationTask<Entity>
        {
            public class Result
            {
                public string Coordinates { get; set; }
            }

            public EntitySpatialIndex2()
            {
                Map = entities => entities.Select(e => new
                {
                    Id = e.Id,
                    Coordinates = CreateSpatialField(e.Geolocation.Lat, e.Geolocation.Lon)
                });

                Spatial("Coordinates", x => x.Cartesian.BoundingBoxIndex());
            }
        }
    }
}
