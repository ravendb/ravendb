using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class SpatialTest2 : RavenTest
	{
		public class Entity
		{
			public double Latitude { set; get; }
			public double Longitude { set; get; }
		}

		public class EntitiesByLocation : AbstractIndexCreationTask<Entity>
		{
			public EntitiesByLocation()
			{
				Map = entities => from entity in entities
								  select new { _ = SpatialGenerate(entity.Latitude, entity.Longitude) };
			}
		}

		[Fact]
		public void WeirdSpatialResults()
		{
			using (IDocumentStore store = NewDocumentStore())
			{
				using (IDocumentSession session = store.OpenSession())
				{
					Entity entity = new Entity()
										{
											Latitude = 45.829507799999988,
											Longitude = -73.800524699999983
										};
					session.Store(entity);
					session.SaveChanges();
				}

				new EntitiesByLocation().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Query<Entity, EntitiesByLocation>()
						.Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
						.ToList();

					// Let's search within a 150km radius
					var results = session.Advanced.LuceneQuery<Entity, EntitiesByLocation>()
						.WithinRadiusOf(radius: 150000 * 0.000621, latitude: 45.831909, longitude: -73.810322)
						// This is less than 1km from the entity
						.SortByDistance()
						.ToList();

					// This works
					Assert.Equal(results.Count, 1);

					// Let's search within a 15km radius
					results = session.Advanced.LuceneQuery<Entity, EntitiesByLocation>()
						.WithinRadiusOf(radius: 15000 * 0.000621, latitude: 45.831909, longitude: -73.810322)
						.SortByDistance()
						.ToList();

					// This fails
					Assert.Equal(results.Count, 1);

					// Let's search within a 1.5km radius
					results = session.Advanced.LuceneQuery<Entity, EntitiesByLocation>()
						.WithinRadiusOf(radius: 1500 * 0.000621, latitude: 45.831909, longitude: -73.810322)
						.SortByDistance()
						.ToList();

					// This fails
					Assert.Equal(results.Count, 1);
				}
			}
		}
	}
}
