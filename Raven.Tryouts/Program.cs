using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace ConsoleApplication4
{
	class Program
	{
		public class Location
		{
			public long Latitude { get; set; }

			public long Longitude { get; set; }
		}

		public class City
		{
			public string Name { get; set; }
			public Location LatLng { get; set; }
		}

		public class City_SpacialIndex : AbstractIndexCreationTask<City>
		{
			public City_SpacialIndex()
			{
				Map = city => from e in city
							  select new
							  {
								  Name = e.Name,
								  __ = SpatialGenerate("Coordinates", e.LatLng.Latitude, e.LatLng.Longitude)
							  };
			}
		}

		private static void Main(string[] args)
		{
			using (var store = new DocumentStore
			{
				Url = "http://localhost.fiddler:8080",
				DefaultDatabase = "Cities"
			}.Initialize())
			{
				double londonLat = 51.520097;
				double londonLng = -0.123571;
				new City_SpacialIndex().Execute(store);

				//				try
				//				{
				for (int j = 0; j < 10; j++)
				{
					Parallel.For(1, 10, async (i) =>
					{
						using (var session = store.OpenAsyncSession())
						{
							var results = await session.Advanced.AsyncDocumentQuery<dynamic, City_SpacialIndex>()
								.WithinRadiusOf("Coordinates", 1000.0, londonLat, londonLng)
								.SortByDistance()
								.Take(20)
								.ToListAsync();

							if (results == null || results.Count == 0)
								throw new Exception("FooBar!");
						}
					});
					
				}
				//				}
				//				catch (Exception e)
				//				{
				//					Debugger.Launch();
				//				}
			}
		}

	}
}
