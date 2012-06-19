extern alias database;
using System;
using System.Linq;
using System.Net;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Authentication;
using Xunit;

namespace Raven.Bundles.Tests.Authentication
{
	public class AsyncSimpleLogin : AuthenticationTest
	{
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
			
			using (var session = store.OpenAsyncSession())
			{
				session.Store(new { Id ="Hal2001",  Name = "Sprite", Age = 321 });
				session.SaveChangesAsync().Wait();
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
					AllowedDatabases = new[] {"*"}
				}.SetPassword("abc"), "Raven/Users/Ayende");
				session.SaveChanges();
			}

			for (int i = 0; i < 5; i++)
			{
				using (var session = store.OpenAsyncSession())
				{
					session.Store(new { Id = "Hal2001", Name = "Sprite", Age = 321 }, "Hal2001");
					session.SaveChangesAsync().Wait();
				}
			}

			var oAuthClientCredentialsTokenResponder = embeddedStore.HttpServer.RequestResponders.OfType<database::Raven.Database.Server.Security.OAuth.OAuthClientCredentialsTokenResponder>().First();
			Assert.Equal(1, oAuthClientCredentialsTokenResponder.NumberOfTokensIssued);
		}

		[Fact]
		public void WillGetAnErrorWhenTryingToLoginIfUserDoesNotExists()
		{
			store.Credentials = new NetworkCredential("Ayende", "abc");

			using (var session = store.OpenAsyncSession())
			{
				session.Store(new { Name = "Sprite", Age = 321 });
				var webException = (WebException)Assert.Throws<AggregateException>(() => session.SaveChangesAsync().Wait()).ExtractSingleInnerException();
				Assert.Equal(HttpStatusCode.Unauthorized, ((HttpWebResponse) webException.Response).StatusCode);
			}
		}
	}
}