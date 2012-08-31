using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class TakeQueries : RavenTest
	{
		[Fact]
		public void ShouldGetNoResultsOnTake0()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}	

				using(var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<User>().Customize(x=>x.WaitForNonStaleResults()).ToList());
					Assert.Empty(session.Query<User>().Take(0).ToList());
				}
			}
		}

		[Fact]
		public void ShouldGetNoResultsOnTakeMinus1()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<User>().Customize(x => x.WaitForNonStaleResults()).ToList());
					Assert.Empty(session.Query<User>().Take(-1).ToList());
				}
			}
		}
	}
}
