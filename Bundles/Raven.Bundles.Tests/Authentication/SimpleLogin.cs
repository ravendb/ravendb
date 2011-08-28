using System.Linq;
using System.Net;
using Raven.Bundles.Authentication;
using Raven.Http.Security.OAuth;
using Xunit;

namespace Raven.Bundles.Tests.Authentication
{
	public class SimpleLogin : AuthenticationTest
	{
		[Fact]
		public void CanLogin()
		{
			using(var session = embeddedStore.OpenSession())
			{
				session.Store(new AuthenticationUser
				{
					Name = "Ayende",
					Id = "Raven/Users/Ayende",
					AllowedDatabases = new[] {"*"}
				}.SetPassword("abc"));
				session.SaveChanges();
			}

			var req = (HttpWebRequest) WebRequest.Create(embeddedStore.Configuration.ServerUrl + "OAuth/AccessToken");
			var response = req
				.WithBasicCredentials("Ayende", "abc")
				.WithConentType("application/json;charset=UTF-8")
				.WithHeader("grant_type", "client_credentials")
				.MakeRequest()
				.ReadToEnd();

			AccessTokenBody body;
			Assert.True(AccessToken.TryParseBody(embeddedStore.Configuration.OAuthTokenCertificate, response, out body));
		}

		[Fact]
		public void CanLoginViaClientApi()
		{
			store.Credentials = new NetworkCredential("Ayende", "abc");

			using (var session = embeddedStore.OpenSession())
			{
			    session.Store(new AuthenticationUser
			    {
			        Name = "Ayende",
			        Id = "Raven/Users/Ayende",
			        AllowedDatabases = new[] { "*" }
			    }.SetPassword("abc"));
			    session.SaveChanges();
			}
			
			using (var session = store.OpenSession())
			{
				session.Store(new { Id ="Hal2001",  Name = "Sprite", Age = 321 });
				session.SaveChanges();
			}

			using (var session = store.OpenSession())
			{
				Assert.Equal("Sprite", session.Load<dynamic>("Hal2001").Name);
			}
		}

		[Fact]
		public void WillRememberToken()
		{
			store.Credentials = new NetworkCredential("Ayende", "abc");

			using (var session = embeddedStore.OpenSession())
			{
				session.Store(new AuthenticationUser
				{
					Name = "Ayende",
					Id = "Raven/Users/Ayende",
					AllowedDatabases = new[] { "*" }
				}.SetPassword("abc"));
				session.SaveChanges();
			}

			for (int i = 0; i < 5; i++)
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Id = "Hal2001", Name = "Sprite", Age = 321 });
					session.SaveChanges();
				}
			}

			var oAuthClientCredentialsTokenResponder = embeddedStore.HttpServer.RequestResponders.OfType<OAuthClientCredentialsTokenResponder>().First();
			Assert.Equal(1, oAuthClientCredentialsTokenResponder.NumberOfTokensIssued);
		}

		[Fact]
		public void WillGetAnErrorWhenTryingToLoginIfUserDoesNotExists()
		{
			store.Credentials = new NetworkCredential("Ayende", "abc");

			using (var session = store.OpenSession())
			{
				session.Store(new { Name = "Sprite", Age = 321 });
				var webException = Assert.Throws<WebException>(() => session.SaveChanges());
				Assert.Equal(HttpStatusCode.Unauthorized, ((HttpWebResponse) webException.Response).StatusCode);
			}

			//using (var session = embeddedStore.OpenSession())
			//{
			//    session.Store(new AuthenticationUser
			//    {
			//        Name = "Ayende",
			//        Id = "Raven/Users/Ayende",
			//        AllowedDatabases = new[] { "*" }
			//    }.SetPassword("abc"));
			//    session.SaveChanges();
			//}

			

		}
	}
}