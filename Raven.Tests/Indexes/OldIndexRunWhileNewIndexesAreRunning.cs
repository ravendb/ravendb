// -----------------------------------------------------------------------
//  <copyright file="OldIndexRunWhileNewIndexesAreRunning.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Tests.Bugs;
using Xunit;
using System.Linq;

namespace Raven.Tests.Indexes
{
	public class OldIndexRunWhileNewIndexesAreRunning : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.MaxNumberOfItemsToIndexInSingleBatch = 128;
			configuration.InitialNumberOfItemsToIndexInSingleBatch = 128;
		}

		[Fact]
		public void OneBigSave()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1024 * 6; i++)
					{
						session.Store(new User { });
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();

					Assert.Equal(1024 * 6, usersCount);
				}
			}
		}

		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1024 * 6; i++)
					{
						session.Store(new User { });
					}
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();

					Assert.Equal(1024 * 6, usersCount);
				}

				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from user in docs.Users select new { user.Name }"
				});

				using (var session = store.OpenSession())
				{
					while (true) // we have to wait until we _start_ indexing
					{
						var objects = session.Advanced.LuceneQuery<object>("test").Take(1).ToList();
						if (objects.Count > 0)
							break;
						Thread.Sleep(10);
					}
				}

				using (var session = store.OpenSession())
				{
					session.Store(new User { });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{

					var usersCount = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Count();

					Assert.Equal(1024 * 6 + 1, usersCount);
				}
			}
		}
	}
}