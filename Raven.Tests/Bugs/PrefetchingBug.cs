using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class PrefetchingBug : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.MaxNumberOfItemsToIndexInSingleBatch = 2;
			configuration.MaxNumberOfItemsToReduceInSingleBatch = 2;
			configuration.InitialNumberOfItemsToIndexInSingleBatch = 2;
			configuration.InitialNumberOfItemsToReduceInSingleBatch = 2;
		}

		[Fact]
		public void ShouldNotSkipAnything()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new User
						{
							Active = true
						});
					}
					session.SaveChanges();
				}

				WaitForIndexing(store); // waiting until the in memory queue is drained

				using(var session = store.OpenSession())
				{
					var users = session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.Active)
						.ToList();
					Assert.Equal(10, users.Count);
				}
			}
		}
	}
}