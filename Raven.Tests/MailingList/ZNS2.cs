// -----------------------------------------------------------------------
//  <copyright file="ZNS2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class ZNS2 : RavenTest
	{
		public class TestItem
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public int Area { get; set; }
			public EventDate[] Dates { get; set; }

			public override int GetHashCode()
			{
				return this.Id.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				if (obj != null && obj is TestItem)
					return ((TestItem)obj).Id == this.Id;
				return false;
			}
		}

		public class EventDate
		{
			public DateTime Date { get; set; }
			public string Time { get; set; }
		}

		[Fact]
		public void Can_SortAndPageMultipleDates()
		{
			using (var store = new EmbeddableDocumentStore { RunInMemory = true })
			{
				//Create an index
				store.Initialize();
				store.DatabaseCommands.PutIndex("TestItemsIndex", new Raven.Abstractions.Indexing.IndexDefinition
				{
					Name = "TestItemsIndex",
					Map = @"from item in docs.TestItems
                        from d in item.Dates.Select((Func<dynamic,dynamic>)(x => x.Date)).Distinct().DefaultIfEmpty()
                        select new {Id = item.Id, Name = item.Name, EventDate = d, Area = item.Area};",
					Stores = { { "EventDate", FieldStorage.No } }
				}, true);

				//Insert some events at random dates
				var size = 50;
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < size; i++)
					{
						var r = new System.Random();
						session.Store(new TestItem()
						{
							Id = "testitems/" + 1000 + i,
							Name = "Event Number " + (1000 + i),
							Area = r.Next(1, 3),
							Dates = null
						});
					}
				}

				using (var session = store.OpenSession())
				{
					for (int i = 1; i <= size; i++)
					{
						var r = new System.Random(i);
						var dates = new List<DateTime>();
						for (var j = 0; j < 5; j++)
						{
							dates.Add(new DateTime(2012, r.Next(1, 12), r.Next(1, 28)));
						}

						session.Store(new TestItem()
						{
							Id = "testitems/" + i,
							Name = "Event Number " + i,
							Area = r.Next(1, 3),
							Dates = dates.Select(x => new EventDate() { Date = x }).ToArray()
						});
					}
					session.SaveChanges();
				}

				//Get all results
				RavenQueryStatistics stats;
				List<TestItem> result = null;
				using (var session = store.OpenSession())
				{
					result = session.Advanced.LuceneQuery<TestItem>("TestItemsIndex")
						.Statistics(out stats)
						.WaitForNonStaleResults()
						.WhereBetweenOrEqual("EventDate", DateTime.Parse("2012-02-01"), DateTime.Parse("2012-09-01"))
						.AndAlso()
						.WhereEquals("Area", 2)
						.OrderBy("EventDate")
						.Take(1000)
						.ToList();
				}

				//Get all results, paged
				List<TestItem> paged = new List<TestItem>();
				RavenQueryStatistics stats2;
				int sum = result.Count;
				int skip = 0;
				var take = 10;
				for (int i = 0; i < sum; i += take)
				{
					using (var session = store.OpenSession())
					{
						var r = session.Advanced.LuceneQuery<TestItem>("TestItemsIndex")
							.Statistics(out stats2)
							.WhereBetweenOrEqual("EventDate", DateTime.Parse("2012-02-01"), DateTime.Parse("2012-09-01"))
							.AndAlso()
							.WhereEquals("Area", 2)
							.OrderBy("EventDate")
							.Skip(paged.Count)
							.Take(take)
							.ToList();
						skip += stats2.SkippedResults;
						paged.AddRange(r);
					}
				}


				Assert.Equal(result.Select(x=>x.Id),paged.Select(x=>x.Id));
			}
		}
	}
}