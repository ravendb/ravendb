using Raven.Client;
using Xunit;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class WithAs : RavenTest
	{
		[Fact]
		public void WillAutomaticallyGenerateSelect()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new User
					{
						Age = 15,
						Email = "ayende"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var array = session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.AsProjection<AgeAndEmail>()
						.ToArray();

					Assert.Equal(1, array.Length);
					Assert.Equal(15, array[0].Age);
					Assert.Equal("ayende", array[0].Email);
				}
			}
		}

		public class AgeAndEmail
		{
			public int Age { get; set; }
			public string Email { get; set; }
		}
	}
}