// -----------------------------------------------------------------------
//  <copyright file="ZNS.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class ZNS : RavenTest
	{
		public class TestItem
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public EventDate[] Dates { get; set; }

			public override string ToString()
			{
				return Id;
			}

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

			public override string ToString()
			{
				return Date.ToShortDateString();
			} 
		}

		[Fact]
		public void Can_SortAndPageMultipleDates()
		{
			using (var store = NewDocumentStore())
			{
				//Create an index
				store.Initialize();
				store.DatabaseCommands.PutIndex("TestItemsIndex", new Raven.Abstractions.Indexing.IndexDefinition
				{
					Name = "TestItemsIndex",
					Map = @"from item in docs.TestItems
                        from d in item.Dates.Select((Func<dynamic,dynamic>)(x => x.Date)).Distinct()
                        select new {Id = item.Id, Name = item.Name, EventDate = d};",
						Stores = {{"EventDate", FieldStorage.Yes}}
				}, true);

				//Insert some events at random dates
				using (var session = store.OpenSession())
				{
					for (int i = 1; i <= 50; i++)
					{
						var dates = new List<DateTime>();
						for (var j = 0; j < 5; j++)
						{
							var r = new System.Random(i*j);
							;
							dates.Add(new DateTime(2012, r.Next(1,12), r.Next(1,31)));
						}

						session.Store(new TestItem()
						{
							Id = "testitems/" + i,
							Name = "Event Number " + i,
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
						.WhereBetweenOrEqual("EventDate", DateTime.Parse("2012-03-01"), DateTime.Parse("2012-06-01"))
						.OrderBy("EventDate")
						.Take(1000)
						.ToList();
				}

				//Get all results, paged
				List<TestItem> pagedResult = new List<TestItem>();
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
							.WhereBetweenOrEqual("EventDate", DateTime.Parse("2012-03-01"), DateTime.Parse("2012-06-01"))
							.OrderBy("EventDate")
							.Take(take)
							.Skip(pagedResult.Count)
							.ToList();
						skip += stats2.SkippedResults;
						pagedResult.AddRange(r);
					}
				}


				//Assert "all" results are equal to paged results
				Assert.Equal(result.Select(x=>x.Id).ToArray(), pagedResult.Select(x=>x.Id).ToArray());
			}
		}
	}
}