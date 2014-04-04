using System.Linq;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Spatial.JsonConverters.GeoJson;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class GeoJsonConverterTests : RavenTest
	{
		public IDocumentStore NewDocumentStore()
		{
			var store = new EmbeddableDocumentStore {RunInMemory = true};
			store.Conventions.CustomizeJsonSerializer = serializer =>
			{
				serializer.Converters.Add(new ICRSObjectConverter());
				serializer.Converters.Add(new FeatureCollectionConverter());
				serializer.Converters.Add(new FeatureConverter());
				serializer.Converters.Add(new AttributesTableConverter());
				serializer.Converters.Add(new GeometryConverter());
				serializer.Converters.Add(new GeometryArrayConverter());
				serializer.Converters.Add(new CoordinateConverter());
				serializer.Converters.Add(new EnvelopeConverter());
			};
			store.Initialize();
			store.ExecuteIndex(new CartesianIndex());
			return store;
		}

		[Fact]
		public void Point()
		{
			var point = new Point(50, 50);
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Geometry = point });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var doc = session.Query<SpatialDoc>().First();
					Assert.Equal(point, doc.Geometry);
				}

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, CartesianIndex>()
										  .Spatial(x => x.Geometry, x => x.WithinRadiusOf(20, 50, 50))
										  .Any();

					Assert.True(matches);
				}
			}
		}

		[Fact]
		public void LineString()
		{
			var lineString = new LineString(new[]
				                                {
					                                new Coordinate(1850, 1850),
					                                new Coordinate(1950, 1850),
					                                new Coordinate(1950, 1950),
					                                new Coordinate(1850, 1950),
				                                });

			using (var store = NewDocumentStore())
			{

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Geometry = lineString });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var doc = session.Query<SpatialDoc>().First();
					Assert.Equal(lineString, doc.Geometry);
				}

				using (var session = store.OpenSession())
				{
					var lineString2 = new LineString(new[]
					                                {
						                                new Coordinate(1800, 1900),
						                                new Coordinate(1950, 2000),
					                                });

					var matches1 = session.Query<SpatialDoc, CartesianIndex>()
										  .Spatial(x => x.Geometry, x => x.Intersects(lineString2))
										  .Any();

					Assert.True(matches1);
				}
			}
		}

		[Fact]
		public void Polygon()
		{
			var polygon = new Polygon(new LinearRing(new[]
			                                         {
				                                         new Coordinate(1850, 1850),
				                                         new Coordinate(1950, 1850),
				                                         new Coordinate(1950, 1950),
				                                         new Coordinate(1850, 1950),
				                                         new Coordinate(1850, 1850),
			                                         }));

			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { Geometry = polygon });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var doc = session.Query<SpatialDoc>().First();
					Assert.Equal(polygon, doc.Geometry);
				}

				using (var session = store.OpenSession())
				{
					var matches = session.Query<SpatialDoc, CartesianIndex>()
										  .Spatial(x => x.Geometry, x => x.Intersects(new Point(1900, 1900)))
										  .Any();

					Assert.True(matches);
				}
			}
		}

		public class SpatialDoc
		{
			public string Id { get; set; }
			public IGeometry Geometry { get; set; }
		}

		public class CartesianIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public CartesianIndex()
			{
				Map = docs => from doc in docs select new { doc.Geometry };

				Spatial(x => x.Geometry, x => x.Cartesian.QuadPrefixTreeIndex(12, new SpatialBounds(0, 0, 2000, 2000)));
			}
		}
	}
}