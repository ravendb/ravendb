using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class DataSetIndexTest : RavenTestBase
	{
		private const int MaxNumberOfItemsInDataSet = 50;

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.MaxIndexOutputsPerDocument = 100;
        }

		[Fact]
		public void can_execute_query_default()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new DataSetIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					CreateDataSet(session, "stations/rtl", "T1");
					CreateDataSet(session, "stations/rtl", "T2");
					CreateDataSet(session, "stations/energy", "EX");
				}

                WaitForUserToContinueTheTest(store);

				using (var session = store.OpenSession())
				{
                    var query = session.Advanced.DocumentQuery<DataSetIndex.Result, DataSetIndex>()
								.WaitForNonStaleResults()
								.AddOrder("Split_N1_Range", true, typeof(double))
								.SelectFields<dynamic>("SongId", "Title", "Interpret", "Year", "Attributes", "SID", "SetId");
					var result = query.ToList();
					Assert.Equal("songs/50", result.First().SongId); //GREEN
				}
			}
		}

		[Fact]
		public void can_execute_query_lazily()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new DataSetIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					CreateDataSet(session, "stations/rtl", "T1");
					CreateDataSet(session, "stations/rtl", "T2");
					CreateDataSet(session, "stations/energy", "EX");
				}


				using (var session = store.OpenSession())
				{
                    var query = session.Advanced.DocumentQuery<DataSetIndex.Result, DataSetIndex>()
								.WaitForNonStaleResults()
								.AddOrder("Split_N1_Range", true, typeof(double))
								.SelectFields<dynamic>("SongId", "Title", "Interpret", "Year", "Attributes", "SID", "SetId");
					var result = query.ToList();
					Assert.Equal("songs/50", result.First().SongId); //GREEN
				}

				using (var session = store.OpenSession())
				{
                    var query = session.Advanced.DocumentQuery<DataSetIndex.Result, DataSetIndex>()
								.WaitForNonStaleResults()
								.AddOrder("Split_N1_Range", true, typeof(double))
								.SelectFields<dynamic>("SongId", "Title", "Interpret", "Year", "Attributes", "SID", "SetId");
					var result = query.Lazily().Value.ToList();
					Assert.Equal("songs/50", result.First().SongId); //RED! (:
				}
			}
		}

		public class DataSet
		{
			public string Id { get; set; }
			public List<Item> Items { get; set; }
			public string StationId { get; set; }
			public DateTime Date { get; set; }
		}

		public class Item
		{
			public List<Attribute> Attributes { get; set; }
			public string SongId { get; set; }
		}

		public class Attribute
		{
			public Attribute() { }
			public Attribute(string name, object value)
			{
				Name = name;
				Value = value;
			}
			public string Name { get; set; }
			public object Value { get; set; }
		}


		private static void CreateDataSet(IDocumentSession session, string stationId, string datasetKey)
		{
			var set = new DataSet
			{
				Id = stationId + "/test/" + datasetKey,
				StationId = stationId,
				Date = DateTime.UtcNow,
				Items = Enumerable.Range(1, MaxNumberOfItemsInDataSet).Select(x => new Item
				{
					SongId = "songs/" + x,
					Attributes = new[]
						{
							new Attribute("Split_N1", x*0.99d ),
							new Attribute("Split_N4",x*0.01d),
							new Attribute("SoundCode","Rock"),
							new Attribute("Kat","T" + x)
						}.ToList()
				}).ToList()
			};
			session.Store(set);
			session.SaveChanges();
		}

		public class DataSetIndex : AbstractIndexCreationTask<DataSet, DataSetIndex.Result>
		{
			public class Result
			{
				public string SetId { get; set; }
				public string SongId { get; set; }
				public string StationId { get; set; }
				public Attribute[] Attributes { get; set; }
				public DateTime Date { get; set; }
			}

			public DataSetIndex()
			{
				Map = sets =>
					  from set in sets
					  from item in set.Items
					  select new
					  {
						  SongId = item.SongId,
						  SetId = set.Id,
						  StationId = set.StationId,
						  Date = set.Date,
						  item.Attributes,
						  _ = "ignore"
					  };

				Reduce = results =>
						 from result in results
						 group result by new { result.SongId, result.StationId }
							 into g
							 select new
							 {
								 SongId = g.Key.SongId,
								 StationId = g.Key.StationId,
								 Date = g.OrderByDescending(x => x.Date).Select(x => x.Date).FirstOrDefault(),
								 SetId = g.OrderByDescending(x => x.Date).Select(x => x.SetId).FirstOrDefault(),
								 Attributes = g.OrderByDescending(x => x.Date).First().Attributes,
								 _ = g.OrderByDescending(x => x.Date).First().Attributes.Select(x => CreateField(x.Name, x.Value))
							 };

				Sort(x => x.Date, SortOptions.String);

				Stores = new Dictionary<Expression<Func<Result, object>>, FieldStorage>()
							 {
								 { e=>e.SongId, FieldStorage.Yes},
								 { e=>e.SetId, FieldStorage.Yes},
								 { e=>e.Attributes, FieldStorage.Yes},
								 { e=>e.StationId, FieldStorage.Yes}
							 };

				MaxIndexOutputsPerDocument = MaxNumberOfItemsInDataSet;
			}
		}

	}
}