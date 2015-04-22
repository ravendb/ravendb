// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2944.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2944 : RavenTest
	{
		private const int MaxNumberOfItemsToProcessInTestIndexes = 256;

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings[Constants.MaxNumberOfItemsToProcessInTestIndexes] = MaxNumberOfItemsToProcessInTestIndexes.ToString(CultureInfo.InvariantCulture);
		}

		private class Order
		{
			public string Id { get; set; }
			public string Company { get; set; }
			public string Employee { get; set; }
			public DateTime OrderedAt { get; set; }
			public DateTime RequireAt { get; set; }
			public DateTime? ShippedAt { get; set; }
			public string ShipVia { get; set; }
			public decimal Freight { get; set; }
		}

		private class Test_Orders_ByCompany : AbstractIndexCreationTask<Order>
		{
			public Test_Orders_ByCompany()
			{
				Map = orders => from order in orders select new { order.Company };
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				var indexDefinition = base.CreateIndexDefinition();
				indexDefinition.IsTestIndex = true;
				return indexDefinition;
			}
		}

		private class Test_Orders_Count : AbstractIndexCreationTask<Order, Test_Orders_Count.Result>
		{
			public class Result
			{
				public string Company { get; set; }

				public int Count { get; set; }
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				var indexDefinition = base.CreateIndexDefinition();
				indexDefinition.IsTestIndex = true;
				return indexDefinition;
			}

			public Test_Orders_Count()
			{
				Map = orders => from order in orders select new { order.Company, Count = 1 };

				Reduce = results => from result in results group result by result.Company into g select new { Company = g.Key, Count = g.Sum(x => x.Count) };
			}
		}

		[Fact]
		public void CanCreateTestMapIndexes()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				new Test_Orders_ByCompany().Execute(store);

				for (var i = 0; i < 100; i++)
				{
					using (var session = store.OpenSession())
					{
						var count = session
							.Query<Order, Test_Orders_ByCompany>()
							.Count();

						if (count == MaxNumberOfItemsToProcessInTestIndexes)
							return;
					}

					Thread.Sleep(100);
				}

				throw new InvalidOperationException("Should not happen.");
			}
		}

		[Fact]
		public void CanCreateTestMapReduceIndexes()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				new Test_Orders_Count().Execute(store);

				for (var i = 0; i < 100; i++)
				{
					using (var session = store.OpenSession())
					{
						var results = session
							.Query<Test_Orders_Count.Result, Test_Orders_Count>()
							.ToList();

						var count = results.Sum(x => x.Count);

						if (count == MaxNumberOfItemsToProcessInTestIndexes)
							return;
					}

					Thread.Sleep(100);
				}

				WaitForUserToContinueTheTest();

				throw new InvalidOperationException("Should not happen.");
			}
		}
	}
}