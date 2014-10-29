//-----------------------------------------------------------------------
// <copyright file="SpatialIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Common;

using Spatial4n.Core.Context.Nts;
using Spatial4n.Core.Shapes;
using Xunit;
using System.Linq;
using SpatialRelation = Raven.Abstractions.Indexing.SpatialRelation;

namespace Raven.Tests.Spatial
{
	public class SpatialIndexTest : RavenTest
	{
		private readonly DocumentStore store;

		public SpatialIndexTest()
		{
            store = NewRemoteDocumentStore(databaseName: "SpatialIndexTest");
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		// same test as in Spatial.Net test cartesian
		[Fact]
		public void CanPerformSpatialSearch()
		{
			var indexDefinition = new IndexDefinition
			{
				Map = "from e in docs.Events select new { Tag = \"Event\", _ = SpatialGenerate(e.Latitude, e.Longitude) }",
				Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
			};

			store.DatabaseCommands.PutIndex("eventsByLatLng", indexDefinition);

			var events = SpatialIndexTestHelper.GetEvents();

			for (int i = 0; i < events.Length; i++)
			{
				store.DatabaseCommands.Put("Events/" + (i + 1), null,
					RavenJObject.FromObject(events[i]),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"));
			}

			const double lat = 38.96939, lng = -77.386398;
			const double radiusInKm = 6.0*1.609344;
			QueryResult queryResult;
			do
			{
				queryResult = store.DatabaseCommands.Query("eventsByLatLng", new SpatialIndexQuery()
				{
					Query = "Tag:[[Event]]",
					QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(lat, lng, radiusInKm),
					SpatialRelation = SpatialRelation.Within,
					SpatialFieldName = Constants.DefaultSpatialFieldName,
					SortedFields = new[] { new SortedField("__distance"), }
				}, null);
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);

			var expected = events.Count(e => GetGeographicalDistance(lat, lng, e.Latitude, e.Longitude) <= radiusInKm);

			Assert.Equal(expected, queryResult.Results.Count);
			Assert.Equal(7, queryResult.Results.Count);

			double previous = 0;
			foreach (var r in queryResult.Results)
			{
				Event e = r.JsonDeserialization<Event>();

				double distance = GetGeographicalDistance(lat, lng, e.Latitude, e.Longitude);
				Console.WriteLine("Venue: " + e.Venue + ", Distance " + distance);

				Assert.True(distance < radiusInKm);
				Assert.True(distance >= previous);
				previous = distance;
			}
		}

		[Fact]
		public void CanPerformSpatialSearchWithNulls()
		{
			var indexDefinition = new IndexDefinition
			{
				Map = "from e in docs.Events select new { Tag = \"Event\", _ = SpatialGenerate(e.Latitude, e.Longitude) }",
				Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
			};

			store.DatabaseCommands.PutIndex("eventsByLatLng", indexDefinition);

            store.DatabaseCommands.Put("Events/1", null,
                RavenJObject.Parse(@"{""Venue"": ""Jimmy's Old Town Tavern"", ""Latitude"": null, ""Longitude"": 35 }"),
                RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"));

            store.DatabaseCommands.Put("Events/2", null,
                RavenJObject.Parse(@"{""Venue"": ""Jimmy's Old Town Tavern"", ""Latitude"": 30, ""Longitude"": null }"),
                RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"));

			store.DatabaseCommands.Put("Events/3", null,
				RavenJObject.Parse(@"{""Venue"": ""Jimmy's Old Town Tavern"", ""Latitude"": null, ""Longitude"": null }"),
				RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"));

			QueryResult queryResult;
			do
			{
				queryResult = store.DatabaseCommands.Query("eventsByLatLng", new IndexQuery()
				{
					Query = "Tag:[[Event]]",
					SortedFields = new[] { new SortedField("__distance"), }
				}, null);
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);

			Assert.Equal(3, queryResult.Results.Count);
		}

		[Fact]
		public void CanSortByDistanceAndAnotherProp()
		{
			var indexDefinition = new IndexDefinition
			{
				Map = "from e in docs.Events select new { e.Venue, _ = SpatialGenerate(e.Latitude, e.Longitude) }",
				Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
			};

			store.DatabaseCommands.PutIndex("eventsByLatLng", indexDefinition);

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
				store.DatabaseCommands.Put("Events/" + (i + 1), null,
					RavenJObject.FromObject(events[i]),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"));
			}

			const double lat = 38.96939, lng = -77.386398;
			const double radius = 6.0;
			QueryResult queryResult;
			do
			{
				queryResult = store.DatabaseCommands.Query("eventsByLatLng", new SpatialIndexQuery()
				{
					QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(lat, lng, radius),
					SpatialRelation = SpatialRelation.Within,
					SpatialFieldName = Constants.DefaultSpatialFieldName,
					SortedFields = new[]
					{
						new SortedField("__distance"), 
						new SortedField("Venue"),
					}
				}, null);
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);

			Assert.Equal(9, queryResult.Results.Count);

			var expectedOrder = new[] { "a/2", "b/2", "c/2", "a/1", "b/1", "c/1", "a/3", "b/3", "c/3" };
			Assert.Equal(expectedOrder.Length, queryResult.Results.Count);
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
				Map = "from e in docs.Events select new { e.Venue, _ = SpatialGenerate(e.Latitude, e.Longitude) }",
				Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
			};

			store.DatabaseCommands.PutIndex("eventsByLatLng", indexDefinition);

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
				store.DatabaseCommands.Put("Events/" + (i + 1), null,
					RavenJObject.FromObject(events[i]),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"));
			}

			const double lat = 38.96939, lng = -77.386398;
			const double radius = 6.0;
			QueryResult queryResult;
			do
			{
				queryResult = store.DatabaseCommands.Query("eventsByLatLng", new SpatialIndexQuery()
				{
					QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(lat, lng, radius),
					SpatialRelation = SpatialRelation.Within,
					SpatialFieldName = Constants.DefaultSpatialFieldName,
					SortedFields = new[]
					{
						new SortedField("Venue"),
						new SortedField("__distance"), 
					}
				}, null);
				if (queryResult.IsStale)
					Thread.Sleep(100);
			} while (queryResult.IsStale);


			var expectedOrder = new[] { "Events/5", "Events/4", "Events/6", "Events/2", "Events/1", "Events/3", };
			Assert.Equal(expectedOrder.Length, queryResult.Results.Count);
			for (int i = 0; i < queryResult.Results.Count; i++)
			{
				Assert.Equal(expectedOrder[i], queryResult.Results[i].Value<RavenJObject>("@metadata").Value<string>("@id"));
			}
		}

		/// <summary>
		/// The International Union of Geodesy and Geophysics says the Earth's mean radius in KM is:
		///
		/// [1] http://en.wikipedia.org/wiki/Earth_radius
		/// </summary>
		private const double EarthMeanRadiusKm = 6371.0087714;
		private const double DegreesToRadians = Math.PI / 180;
		private const double RadiansToDegrees = 1 / DegreesToRadians;

		public static double GetGeographicalDistance(double fromLat, double fromLng, double toLat, double toLng)
		{
			var Context = new NtsSpatialContext(true);
			Point ptFrom = Context.MakePoint(fromLng, fromLat);
			Point ptTo = Context.MakePoint(toLng, toLat);
			var distance = Context.GetDistCalc().Distance(ptFrom, ptTo);
			return (distance / RadiansToDegrees) * EarthMeanRadiusKm;
		}
	}
}
