using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class Boolean : RavenTest
	{
		[Fact]
		public void CanQueryOnNegation()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new User{Active = false});
					s.SaveChanges();	
				}

				using(var s = store.OpenSession())
				{
					Assert.Equal(1, s.Query<User>()
						.Where(x => !x.Active)
						.Count());
				}
			}
		}
	}
}