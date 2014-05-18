// -----------------------------------------------------------------------
//  <copyright file="WaitForStaleOnAbandonedIndexShouldWork.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class WaitForStaleOnAbandonedIndexShouldWork : RavenTest
	{
		[Fact]
		public void QueryAnAbandonedIndexShouldMakeTheIndexPriorityAsNormal()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Product {Name = "Product 1"});
					session.SaveChanges();

					var product = session.Query<Product>()
					                     .Customize(customization => customization.WaitForNonStaleResults())
					                     .FirstOrDefault(p => p.Name == "Product 1");

				}

				var indexName = store.DatabaseCommands.GetIndexNames(0, 25).Single(s => s.StartsWith("Auto/"));

				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new Product {Name = "Another product " + i});
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var product = session.Query<Product>()
										 .Customize(customization => customization.WaitForNonStaleResults())
										 .FirstOrDefault(p => p.Name == "Product 1");
				}

				var statistics = store.DatabaseCommands.GetStatistics();
				var indexStats = statistics.Indexes.Single(stats => stats.PublicName == indexName);
				Assert.Equal(IndexingPriority.Normal, indexStats.Priority);
			}
		}

		public class Product
		{
			public string Name { get; set; }
		}
	}
}