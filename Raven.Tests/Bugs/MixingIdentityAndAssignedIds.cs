using Xunit;

namespace Raven.Tests.Bugs
{
	public class MixingIdentityAndAssignedIds : RavenTest
	{
		[Fact]
		public void WillWork()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new User
					{
						Id = "users/1"
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Store(new User
					{
						Id = "users/"
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					Assert.NotNull(s.Load<User>("users/2"));
				}
			}
		}
	}
}
