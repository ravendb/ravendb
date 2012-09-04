using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Spatial
{
	public class BrainV : RavenTest
	{
		[Fact]
		public void CanPerformSpatialSearchWithNulls()
		{
			using(var store = NewDocumentStore())
			{
				var indexDefinition = new IndexDefinition
				{
					Map = "from e in docs.Events select new { Tag = \"Event\", _ = SpatialGenerate(e.Latitude, e.Longitude) }",
					Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
				};

				store.DocumentDatabase.PutIndex("eventsByLatLng", indexDefinition);

				store.DocumentDatabase.Put("Events/1", null,
					RavenJObject.Parse(@"{""Venue"": ""Jimmy's Old Town Tavern"", ""Latitude"": null, ""Longitude"": null }"),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"), null);

				using(var session = store.OpenSession())
				{
					var objects = session.Query<object>("eventsByLatLng")
						.Customize(x => x.WithinRadiusOf(6, 0, 0))
						.Customize(x => x.WaitForNonStaleResults())
						.ToArray();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.Equal(1, objects.Length);
				}
			}

		}


		[Fact]
		public void CanUseNullCoalescingOperator()
		{
			using (var store = NewDocumentStore())
			{
				var indexDefinition = new IndexDefinition
				{
					Map = "from e in docs.Events select new { Tag = \"Event\", _ = SpatialGenerate(e.Latitude ?? 38.9103000, e.Longitude ?? -77.3942) }",
					Indexes = {
					{ "Tag", FieldIndexing.NotAnalyzed }
				}
				};

				store.DocumentDatabase.PutIndex("eventsByLatLng", indexDefinition);

				store.DocumentDatabase.Put("Events/1", null,
					RavenJObject.Parse(@"{""Venue"": ""Jimmy's Old Town Tavern"", ""Latitude"": null, ""Longitude"": null }"),
					RavenJObject.Parse("{'Raven-Entity-Name': 'Events'}"), null);

				using (var session = store.OpenSession())
				{
					var objects = session.Query<object>("eventsByLatLng")
						.Customize(x => x.WithinRadiusOf(6, 38.9103000, -77.3942))
						.Customize(x => x.WaitForNonStaleResults())
						.ToArray();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.Equal(1, objects.Length);
				}
			}

		}
	}
}