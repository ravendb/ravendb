extern alias database;
using System;
using System.Linq;
using System.Net;
using Raven.Bundles.Authentication;
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
				.WithAccept("application/json;charset=UTF-8")
				.WithHeader("grant_type", "client_credentials")
				.MakeRequest()
				.ReadToEnd();

			database::Raven.Database.Server.Security.OAuth.AccessTokenBody body;
			Assert.True(database::Raven.Database.Server.Security.OAuth.AccessToken.TryParseBody(embeddedStore.Configuration.OAuthTokenCertificate, response, out body));
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
					AllowedDatabases = new[] { "*" }
				}.SetPassword("abc"), "Raven/Users/Ayende");
				session.SaveChanges();
			}

			for (int i = 0; i < 5; i++)
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Id = "Hal2001", Name = "Sprite", Age = 321 }, "Hal2011");
					session.SaveChanges();
				}
			}

			var oAuthClientCredentialsTokenResponder = embeddedStore.DocumentDatabase.RequestResponders.OfType<database::Raven.Database.Server.Security.OAuth.OAuthClientCredentialsTokenResponder>().First();
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
		}
	}
}