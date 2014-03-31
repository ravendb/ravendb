// -----------------------------------------------------------------------
//  <copyright file="DistinctWithPaging.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Client;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class DistinctWithPaging : RavenTest
	{
		public class Item
		{
			public int Val { get; set; }
		}

		public class ItemIndex : AbstractIndexCreationTask<Item>
		{
			public ItemIndex()
			{
				Map = items =>
					  from item in items
					  select new { item.Val };
				Store(x => x.Val, FieldStorage.Yes);
				Sort(x => x.Val, SortOptions.Int);
			}
		}

		[Fact]
		public void CanWorkProperly()
		{
			using (var store = NewDocumentStore())
			{
				new ItemIndex().Execute(store);
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 25; i++)
					{
						session.Store(new Item { Val = i + 1 });
						session.Store(new Item { Val = i + 1 });
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					RavenQueryStatistics stats;
					var results = session.Query<Item, ItemIndex>()
										 .Statistics(out stats)
										 .Customize(x => x.WaitForNonStaleResults())
										 .OrderBy(t => t.Val)
										 .Select(t => t.Val)
										 .Distinct()
										 .Skip(0)
										 .Take(10)
										 .ToList();

					Assert.Equal(Enumerable.Range(1, 10), results);

					results = session.Query<Item, ItemIndex>()
										.Statistics(out stats)
										.OrderBy(t => t.Val)
										.Select(t => t.Val)
										.Distinct()
										.Skip(results.Count)
										.Take(10)
										.ToList();

					Assert.Equal(Enumerable.Range(11, 10), results);

				}
			}
		}
	}
}