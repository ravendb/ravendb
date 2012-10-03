using System.Linq;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Rudolph : RavenTest
	{
		public class User
		{
			public string Name { get; set; }
			public string Email { get; set; }

			public Login[] Logins { get; set; }
		}

		public class Login
		{
			public string OpenIdIdentifier { get; set; }
		}

		public class Identifier
		{
			private string str;

			public static implicit operator string(Identifier identifier)
			{
				return identifier.str;
			}

			public static implicit operator Identifier(string identifier)
			{
				return new Identifier {str = identifier};
			}
		}

		[Fact]
		public void CanUseImplicitIdentifier()
		{
			Identifier identifier = "http://openid";
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					var user = new User()
					{
						Name = "Test",
						Logins = new Login[] { new Login() { OpenIdIdentifier = identifier } }
					};

					s.Store(user);
					s.SaveChanges();

				}
				using(var s = store.OpenSession())
				{
					var query = from u in s.Query<User>()
								where u.Logins.Any(x => x.OpenIdIdentifier == identifier)
								select u;
					Assert.NotNull(query.SingleOrDefault()); // will throw
				}
			}
		}
	}
}