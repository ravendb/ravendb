using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class Nick : RavenTest
	{
		public class MySpatialDocument
		{
			public string Id { get; set; }
			public double Latitude { get; set; }
			public double Longitude { get; set; }
		}

		public class MySpatialIndex : Raven.Client.Indexes.AbstractIndexCreationTask<MySpatialDocument>
		{
			public MySpatialIndex()
			{
				Map = entities => from entity in entities
								  select new
								  {
									  _ = SpatialGenerate(entity.Latitude, entity.Longitude)
								  };
			}
		}

		[Fact]
		public void Test()
		{
			using (IDocumentStore store = NewDocumentStore())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new MySpatialDocument
					{
						Id = "spatials/1",
						Latitude = 48.3708044,
						Longitude = 2.8028712999999925
					});
					session.SaveChanges();
				}

				new MySpatialIndex().Execute(store);

				WaitForIndexing(store);

				// Distance between the 2 tested points is 35.75km
				// (can be checked here: http://www.movable-type.co.uk/scripts/latlong.html

				using (IDocumentSession session = store.OpenSession())
				{
					var result = session.Advanced.LuceneQuery<MySpatialDocument, MySpatialIndex>()
						// 1.025 is for the 2.5% uncertainty at the circle circumference
						.WithinRadiusOf(radius: 35.75 * 1.025, latitude: 48.6003516, longitude: 2.4632387000000335)
						.SingleOrDefault();

					Assert.NotNull(result); // A location should be returned.

					result = session.Advanced.LuceneQuery<MySpatialDocument, MySpatialIndex>()
						.WithinRadiusOf(radius: 30, latitude: 48.6003516, longitude: 2.4632387000000335)
						.SingleOrDefault();

					Assert.Null(result); // No result should be returned.

					result = session.Advanced.LuceneQuery<MySpatialDocument, MySpatialIndex>()
						.WithinRadiusOf(radius: 33, latitude: 48.6003516, longitude: 2.4632387000000335)
						.SingleOrDefault();

					Assert.Null(result); // No result should be returned.

					var shape = SpatialIndexQuery.GetQueryShapeFromLatLon(48.6003516, 2.4632387000000335, 33);
					result = session.Advanced.LuceneQuery<MySpatialDocument, MySpatialIndex>()
						.RelatesToShape(Constants.DefaultSpatialFieldName, shape, SpatialRelation.Intersects, 0)
						.SingleOrDefault();

					Assert.Null(result); // No result should be returned.
				}
			}
		}
	}
}
