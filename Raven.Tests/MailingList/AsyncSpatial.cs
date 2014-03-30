using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class AsyncSpatial : RavenTestBase
	{
		[Fact]
		public async Task SpatialIndexTest()
		{
			using (EmbeddableDocumentStore db = NewDocumentStore())
			{
				new Promos_Index().Execute(db);

				using (IAsyncDocumentSession session = db.OpenAsyncSession())
				{
					await session.StoreAsync(new Promo
					{
						Title = "IPHONES",
						Coordinate = new Coordinate {latitude = 41.145556, longitude = -73.995}
					});
					await session.StoreAsync(new Promo
					{
						Title = "ANDROIDS",
						Coordinate = new Coordinate {latitude = 41.145533, longitude = -73.999}
					});
					await session.StoreAsync(new Promo
					{
						Title = "BLACKBERRY",
						Coordinate = new Coordinate {latitude = 12.233, longitude = -73.995}
					});
					await session.SaveChangesAsync();

					WaitForIndexing(db);

					var result = await session.Query<Promo, Promos_Index>()
					                          .Customize(
						                          x => x.WithinRadiusOf(
							                          radius: 3.0,
							                          latitude: 41.145556,
							                          longitude: -73.995))
					                          .ToListAsync();

					Assert.Equal(2, result.Count);
				}
			}
		}

		public class Coordinate
		{
			public double latitude { get; set; }
			public double longitude { get; set; }
		}

		public class Entity
		{
			public string Id { get; set; }
		}

		public class Promo : Entity
		{
			public string Title { get; set; }

			public Coordinate Coordinate { get; set; }
		}

		public class Promos_Index : AbstractIndexCreationTask<Promo>
		{
			public Promos_Index()
			{
				Map = promos => from p in promos
				                select new
				                {
					                p.Title,
					                p.Coordinate,
					                __ = SpatialGenerate(p.Coordinate.latitude, p.Coordinate.longitude)
				                };
			}
		}
	}
}