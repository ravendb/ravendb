// -----------------------------------------------------------------------
//  <copyright file="RavenDB_863_2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_863_2 : RavenTest
	{
		public class User { public string Name { get; set; } }

		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.InitialNumberOfItemsToReduceInSingleBatch = 2;
			configuration.MaxNumberOfItemsToReduceInSingleBatch = 2;
			configuration.NumberOfItemsToExecuteReduceInSingleStep = 2;
		}

		[Fact]
		public void MapReduceWorksEvenWhenReduceReduceKeysToTakeIsSmall()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from u in docs.Users select new { u.Name, Count = 1 } ",
					Reduce = "from r in results group r by r.Name into g select new { Name = g.Key, Count = g.Sum(x=>x.Count) }"
				});
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						session.Store(new User { Name = "user" });
					}
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var result = session.Query<dynamic>("test").Single();
					Assert.Equal(100, Convert.ToInt32(result.Count));
				}
			}
		}

		[Fact]
		public void MapReduceWorksEvenWhenReduceReduceKeysToTakeIsSmall_WithManyReduceKey()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from u in docs.Users select new { u.Name, Count = 1 } ",
					Reduce = "from r in results group r by r.Name into g select new { Name = g.Key, Count = g.Sum(x=>x.Count) }"
				});
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 100; i++)
					{
						for (int j = 0; j < 10; j++)
						{
							session.Store(new User { Name = "user-" + i });
						}
					}
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<dynamic>("test").ToList();
					foreach (var result in results)
					{
						Assert.Equal(10, Convert.ToInt32(result.Count));
					}
				}
			}
		}
	}
}