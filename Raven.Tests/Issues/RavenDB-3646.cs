using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.SessionState;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Rhino.Mocks;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3646 : RavenTestBase
	{
		[Fact]
		public void QueryWithCusomize()
		{
			using (var store = NewRemoteDocumentStore(fiddler: true))
			{
				StoreData(store);
				store.ExecuteIndex(new Events_SpatialIndex());
				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var rq = session.Query<Events_SpatialIndex.ReduceResult, Events_SpatialIndex>()
						.Statistics(out stats)
						.Customize(
							x => x.WithinRadiusOf("Coordinates", 10000, 1, 1,
								SpatialUnits.Miles));
					var t = 0;

					using (var enumerator = session.Advanced.Stream(rq.AsProjection<Event>()))
					{
						while (enumerator.MoveNext())
						{
							t++;
						}
					}
					Assert.Equal(300, t);
				}
			}

		}
		[Fact]
		public void QueryWithoutCusomize()
		{
			using (var store = NewRemoteDocumentStore(fiddler: true))
			{
				StoreData(store);
				store.ExecuteIndex(new Events_SpatialIndex());
				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var rq = session.Query<Events_SpatialIndex.ReduceResult, Events_SpatialIndex>()
						.Statistics(out stats);

					using (var enumerator = session.Advanced.Stream(rq.AsProjection<Event>()))
					{
						var t = 0;
						while (enumerator.MoveNext())
						{
							t++;
						}
						Assert.Equal(300, t);
					}
				}
			}
		}

		public class Events_SpatialIndex : AbstractIndexCreationTask<Event, Events_SpatialIndex.ReduceResult>
		{
			public class ReduceResult
			{
				public string Name { get; set; }
			}

			public Events_SpatialIndex()
			{
				this.Map = events => from e in events
									 select new
									 {
										 Name = e.Name,
										 __ = SpatialGenerate("Coordinates", e.Latitude, e.Longitude)
									 };
			}
		}

		public class Event
		{
			public string Name { get; set; }
			public double Latitude { get; set; }
			public double Longitude { get; set; }
		}

		public void StoreData(IDocumentStore store)
		{
			using(var session = store.OpenSession())
			{
				for (int i = 0; i < 300; i++)
					session.Store(new Event { Name = "e1" + i, Latitude = 1, Longitude = 1 });
				session.SaveChanges();
			}
		
		}
	}
}
