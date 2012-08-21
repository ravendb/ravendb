using Raven.Tests.Bugs.Queries;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.PoisonIndexes
{
	public class PoisonIndex : RavenTest
	{
		[Fact]
		public void ShouldNotCauseFailures()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new User());

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<Blog>()
						.Customize(x => x.WaitForNonStaleResults())
						.OrderBy(x => x.Category)
						.ToList();

					Assert.NotEmpty(session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList());
				}
			}
		}
	}
}