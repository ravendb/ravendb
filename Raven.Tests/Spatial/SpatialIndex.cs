//-----------------------------------------------------------------------
// <copyright file="SpatialIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
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
					RavenJObject.FromObject(events[i]),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"), null);
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
					SortedFields = new[]{new SortedField("__distance"), }
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

		[Fact]
		public void CanSortByDistanceAndAnotherProp()
		{
			var indexDefinition = new IndexDefinition
			{
				Map = "from e in docs.Events select new { e.Venue, _ = SpatialIndex.Generate(e.Latitude, e.Longitude) }",
				Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
			};

			db.PutIndex("eventsByLatLng", indexDefinition);

			var events = new[]
			{
				new Event("a/1", 38.9579000, -77.3572000),
				new Event("b/1", 38.9579000, -77.3572000),
				new Event("c/1", 38.9579000, -77.3572000),
				new Event("a/2", 38.9690000, -77.3862000),
				new Event("b/2", 38.9690000, -77.3862000),
				new Event("c/2", 38.9690000, -77.3862000),
				new Event("a/3", 38.9510000, -77.4107000),
				new Event("b/3", 38.9510000, -77.4107000),
				new Event("c/3", 38.9510000, -77.4107000),
			};

			for (int i = 0; i < events.Length; i++)
			{
				db.Put("Events/" + (i + 1), null,
					RavenJObject.FromObject(events[i]),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"), null);
			}

			const double lat = 38.96939, lng = -77.386398;
			const double radius = 6.0;
			QueryResult queryResult;
			do
			{
				queryResult = db.Query("eventsByLatLng", new SpatialIndexQuery()
				{
					Latitude = lat,
					Longitude = lng,
					Radius = radius,
					SortedFields = new[]
					{
						new SortedField("__distance"), 
						new SortedField("Venue"),
					}
				});
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);

			Assert.Equal(9, queryResult.Results.Count);

			var expectedOrder = new[] {"a/2", "b/2", "c/2", "a/1", "b/1", "c/1", "a/3", "b/3", "c/3"};

			for (int i = 0; i < queryResult.Results.Count; i++)
			{
				Assert.Equal(expectedOrder[i], queryResult.Results[i].Value<string>("Venue"));
			}
		}


		[Fact]
		public void CanSortByAnotherPropAnddistance()
		{
			var indexDefinition = new IndexDefinition
			{
				Map = "from e in docs.Events select new { e.Venue, _ = SpatialIndex.Generate(e.Latitude, e.Longitude) }",
				Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
			};

			db.PutIndex("eventsByLatLng", indexDefinition);

			var events = new[]
			{
				new Event("b", 38.9579000, -77.3572000),
				new Event("b", 38.9690000, -77.3862000),
				new Event("b", 38.9510000, -77.4107000),
			
				new Event("a", 38.9579000, -77.3572000),
				new Event("a", 38.9690000, -77.3862000),
				new Event("a", 38.9510000, -77.4107000),

		};

			for (int i = 0; i < events.Length; i++)
			{
				db.Put("Events/" + (i + 1), null,
					RavenJObject.FromObject(events[i]),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"), null);
			}

			const double lat = 38.96939, lng = -77.386398;
			const double radius = 6.0;
			QueryResult queryResult;
			do
			{
				queryResult = db.Query("eventsByLatLng", new SpatialIndexQuery()
				{
					Latitude = lat,
					Longitude = lng,
					Radius = radius,
					SortedFields = new[]
					{
						new SortedField("Venue"),
						new SortedField("__distance"), 
					}
				});
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);


			var expectedOrder = new[] { "events/5", "events/4", "events/6", "events/2", "events/1", "events/3", };

			for (int i = 0; i < queryResult.Results.Count; i++)
			{
				Assert.Equal(expectedOrder[i], queryResult.Results[i].Value<RavenJObject>("@metadata").Value<string>("@id"));
			}
		}
	}
}
