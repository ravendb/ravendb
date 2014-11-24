using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Spatial
{
	public class ShapeConverterTests : RavenTest
	{
		[Fact]
		public void Points()
		{
			using (var store = NewDocumentStore())
			{
				store.Initialize();
				store.ExecuteIndex(new CartesianIndex());

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc {
						Geometry = "POLYGON ((1850 1850, 1950 1850, 1950 1950,1850 1950, 1850 1850))"
					});
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches1 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new
						{
							type = "Point",
							coordinates = new[] { 1900, 1900 }
						}))
						.Any();

					Assert.True(matches1);

					var matches2 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new[] { 1900, 1900 }))
						.Any();

					Assert.True(matches2);

					var matches3 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { Latitude = 1900, Longitude = 1900 }))
						.Any();

					Assert.True(matches3);

					var matches4 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { X = 1900, Y = 1900 }))
						.Any();

					Assert.True(matches4);

					var matches5 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { lat = 1900, lng = 1900 }))
						.Any();

					Assert.True(matches5);

					var matches6 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { lat = 1900, Long = 1900 }))
						.Any();

					Assert.True(matches6);

					var matches7 = session.Query<SpatialDoc, CartesianIndex>()
						.Spatial(x => x.Geometry, x => x.Intersects(new { lat = 1900, lon = 1900 }))
						.Any();

					Assert.True(matches7);
				}
			}
		}

		public class SpatialDoc
		{
			public string Id { get; set; }
			public string Geometry { get; set; }
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
