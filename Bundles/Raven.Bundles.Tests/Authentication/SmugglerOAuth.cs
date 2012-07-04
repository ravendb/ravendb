//-----------------------------------------------------------------------
// <copyright file="SmugglerOAuth.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias database;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Bundles.Authentication;
using Raven.Smuggler;
using Xunit;

namespace Raven.Bundles.Tests.Authentication
{
	public class SmugglerOAuth : AuthenticationTest
	{
		private const string File = "SmugglerOAuth.raven";

		public SmugglerOAuth()
		{
			embeddedStore.Configuration.AnonymousUserAccessMode = database::Raven.Database.Server.AnonymousUserAccessMode.None;

			using (var s = embeddedStore.OpenSession())
			{
				s.Store(new AuthenticationUser
							{
								Name = "Ayende",
								Id = "Raven/Users/Ayende",
								AllowedDatabases = new[] {"*"}
							}.SetPassword("abc"));

				for (int i = 0; i < 10; i++)
				{
					s.Store(new Item {});
				}
				s.SaveChanges();
			}
		}

		[Fact]
		public void Export_WithCredentials_WillSuccess()
		{
			var smugglerApi = new SmugglerApi(new SmugglerOptions(), new RavenConnectionStringOptions { Url = store.Url, Credentials = new NetworkCredential("Ayende", "abc") });

			smugglerApi.ExportData(new SmugglerOptions { File = File });
		}

		[Fact]
		public void Export_WithoutCredentials_WillReturnWithStatus401()
		{
			var smugglerApi = new SmugglerApi(new SmugglerOptions(), new RavenConnectionStringOptions { Url = store.Url });

			var webException = Assert.Throws<WebException>(() => smugglerApi.ExportData(new SmugglerOptions { File = File }));
			Assert.Equal(WebExceptionStatus.ProtocolError, webException.Status);
			Assert.Equal(HttpStatusCode.Unauthorized, ((HttpWebResponse)webException.Response).StatusCode);
		}

		public class Item
		{
		}
	}
}