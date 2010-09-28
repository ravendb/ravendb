using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Tests.Storage;
using Xunit;
using Raven.Database.Json;

namespace Raven.Tests.Spatial
{
	public class SpatialIndex : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public SpatialIndex()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			});
			db.SpinBackgroundWorkers();
		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

		// same test as in Spatial.Net test cartisian
		[Fact]
		public void CanPerformSpatialSearch()
		{
			var indexDefinition = new IndexDefinition 
			{
				Map = "from e in docs.Events select new { Tag = \"Event\", _ = SpatialIndex.Generate(e.Latitude, e.Longitude) }",
				Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
			};

			db.PutIndex("eventsByLatLng", indexDefinition);

			var events = SpatialIndexTestHelper.GetEvents();

			for (int i = 0; i < events.Length; i++)
			{				
				db.Put("Events/" + (i + 1), null,
					JObject.FromObject(events[i]),
					JObject.Parse("{'Raven-Entity-Name': 'Events'}"), null);
			}

			const double lat = 38.96939, lng = -77.386398;
			const double radius = 6.0;

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("eventsByLatLng", new SpatialIndexQuery()
				{
					Query = "Tag:[[Event]]",
					Latitude = lat,
					Longitude = lng,
					Radius = radius,
					SortByDistance = true
				});
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);

			Assert.Equal(7, queryResult.Results.Count);

			double previous = 0;
			foreach (var r in queryResult.Results)
			{
				Event e = r.JsonDeserialization<Event>();

				double distance = Raven.Database.Indexing.SpatialIndex.GetDistanceMi(lat, lng, e.Latitude, e.Longitude);

				Console.WriteLine("Venue: " + e.Venue + ", Distance " + distance);

				Assert.True(distance < radius);
				Assert.True(distance >= previous);
				previous = distance;
			}
		}
	}
}
