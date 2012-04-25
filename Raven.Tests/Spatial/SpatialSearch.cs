using System;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Spatial
{
	public class SpatialSearch : LocalClientTest
	{
		private class SpatialIdx : AbstractIndexCreationTask<Event>
		{
			public SpatialIdx()
			{
				Map = docs => from e in docs
							  select new {e.Capacity, e.Venue, e.Date, _ = SpatialIndex.Generate(e.Latitude, e.Longitude)};

				Index(x => x.Venue, FieldIndexing.Analyzed);
			}
		}

		[Fact]
		public void Can_do_spatial_search_with_client_api()
		{
			using (var store = NewDocumentStore())
			{
				new SpatialIdx().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Event("a/1", 38.9579000, -77.3572000, DateTime.Now));
					session.Store(new Event("a/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(1)));
					session.Store(new Event("b/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(2)));
					session.Store(new Event("c/3", 38.9510000, -77.4107000, DateTime.Now.AddYears(3)));
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var events = session.Advanced.LuceneQuery<Event>("SpatialIdx")
						.Statistics(out stats)
						.WhereLessThanOrEqual("Date", DateTimeOffset.Now.AddYears(1))
						.WithinRadiusOf(6.0, 38.96939, -77.386398)
						.OrderByDescending(x => x.Date)
						.ToList();

					Assert.NotEqual(0, stats.TotalResults);
				}
			}
		}

		[Theory]
		[CriticalCultures]
		public void Can_do_spatial_search_with_client_api2(CultureInfo cultureInfo)
		{
			using(new TemporaryCulture(cultureInfo))
			using (var store = NewDocumentStore())
			{
				new SpatialIdx().Execute(store);

				using (var session = store.OpenSession())
				{
					var matchingVenues = session.Query<Event, SpatialIdx>()
						.Customize(x => x
						                	.WithinRadiusOf(5, 38.9103000, -77.3942)
						                	.WaitForNonStaleResultsAsOfNow()
						);

					Assert.Equal(" Lat: 38.9103 Lng: -77.3942 Radius: 5", matchingVenues.ToString());
				}
			}
		}

		[Fact]
		public void Can_do_spatial_search_with_client_api_within_given_capacity()
		{
			using (var store = NewDocumentStore())
			{
				new SpatialIdx().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Event("a/1", 38.9579000, -77.3572000, DateTime.Now, 5000));
					session.Store(new Event("a/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(1), 5000));
					session.Store(new Event("b/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(2), 2000));
					session.Store(new Event("c/3", 38.9510000, -77.4107000, DateTime.Now.AddYears(3), 1500));
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var events = session.Advanced.LuceneQuery<Event>("SpatialIdx")
						.Statistics(out stats)
						.WhereBetweenOrEqual("Capacity", 0, 2000)
						.WithinRadiusOf(6.0, 38.96939, -77.386398)
						.OrderByDescending(x => x.Date)
						.ToList();

					Assert.Equal(2, stats.TotalResults);
				}
			}
		}

		[Fact]
		public void Can_do_spatial_search_with_client_api_addorder()
		{
			using (var store = NewDocumentStore())
			{
				new SpatialIdx().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Event("a/1", 38.9579000, -77.3572000));
					session.Store(new Event("b/1", 38.9579000, -77.3572000));
					session.Store(new Event("c/1", 38.9579000, -77.3572000));
					session.Store(new Event("a/2", 38.9690000, -77.3862000));
					session.Store(new Event("b/2", 38.9690000, -77.3862000));
					session.Store(new Event("c/2", 38.9690000, -77.3862000));
					session.Store(new Event("a/3", 38.9510000, -77.4107000));
					session.Store(new Event("b/3", 38.9510000, -77.4107000));
					session.Store(new Event("c/3", 38.9510000, -77.4107000));
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var events = session.Advanced.LuceneQuery<Event>("SpatialIdx")
						.WithinRadiusOf(6.0, 38.96939, -77.386398)
						.SortByDistance()
						.AddOrder("Venue", false)
						.ToList();

					var expectedOrder = new[] { "a/2", "b/2", "c/2", "a/1", "b/1", "c/1", "a/3", "b/3", "c/3" };
					for (int i = 0; i < events.Count; i++)
					{
						Assert.Equal(expectedOrder[i], events[i].Venue);
					}
				}
			}
		}
	}
}
