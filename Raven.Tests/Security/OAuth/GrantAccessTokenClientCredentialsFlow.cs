using System;
using System.ComponentModel.Composition.Hosting;
using System.Net;
using Raven.Client;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Security.OAuth;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Security.OAuth
{
	/// <summary>
	/// Client credentials flow used to grant access tokens to confidential clients (such as a web server)
	/// http://tools.ietf.org/html/draft-ietf-oauth-v2-20#section-4.4
	/// </summary>
	public class GrantAccessTokenClientCredentialsFlow : RemoteClientTest, IDisposable
	{
		readonly string path;
		const string baseUrl = "http://localhost";
		const string tokenUrl = "/OAuth/AccessToken";
		const int port = 8079;
		const string validClientUsername = "client1";
		const string validClientPassword = "password";

		public GrantAccessTokenClientCredentialsFlow()
		{
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);

		}

		protected override void ModifyConfiguration(RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			ravenConfiguration.OAuthTokenCertificate = CertGenerator.GenerateNewCertificate("RavenDB.Test");
			ravenConfiguration.Catalog.Catalogs.Add(new TypeCatalog(typeof(FakeAuthenticateClient)));
		}

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
			// Do not create the default index "RavenDocumentsByEntityName".
		}

		public class FakeAuthenticateClient : IAuthenticateClient
		{
			public bool Authenticate(DocumentDatabase documentDatabase, string username, string password, out AccessTokenBody.DatabaseAccess[] allowedDatabases)
			{
				allowedDatabases = new[]
				{
					new AccessTokenBody.DatabaseAccess
					{
						TenantId = "*"
					},
				};
				return string.IsNullOrEmpty(password) == false;
			}
		}

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		public HttpWebRequest GetNewValidTokenRequest()
		{
			var request = ((HttpWebRequest)WebRequest.Create(baseUrl + ":" + port + tokenUrl))
				.WithBasicCredentials(baseUrl, validClientUsername, validClientPassword)
				.WithAccept("application/json;charset=UTF-8")
				.WithHeader("grant_type", "client_credentials");

			return request;
		}

		[Fact]
		public void ValidAndAuthorizedRequestShouldBeGrantedAnAccessToken()
		{

			var request = GetNewValidTokenRequest();

			using (var server = GetNewServer())
			using (var response = request.MakeRequest())
			{
				Assert.Equal(HttpStatusCode.OK, response.StatusCode);

				string token = response.ReadToEnd();

				AccessTokenBody body;

				Assert.NotEmpty(token);
				Assert.True(AccessToken.TryParseBody(server.Database.Configuration.OAuthTokenCertificate, token, out body));
				Assert.False(body.IsExpired());
			}


		}

		[Fact]
		public void RequestWithoutUExpectedAcceptShouldBeRejected()
		{
			var request = GetNewValidTokenRequest()
				.WithAccept("text/plain");

			using (var server = GetNewServer())
			using (var response = request.MakeRequest())
			{
				Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

				var result = RavenJObject.Parse(response.ReadToEnd());

				Assert.Contains("error", result.Keys);
				Assert.Equal("invalid_request", result["error"]);
				Assert.Contains("error_description", result.Keys);
				Assert.Contains("Accept", result["error_description"].Value<string>());
			}
		}

		[Fact]
		public void RequestWithoutAGrantTypeShouldBeRejected()
		{
			var request = GetNewValidTokenRequest()
				.WithoutHeader("grant_type");

			using (var server = GetNewServer())
			using (var response = request.MakeRequest())
			{
				Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

				var readToEnd = response.ReadToEnd();
				var result = RavenJObject.Parse(readToEnd);

				Assert.Contains("error", result.Keys);
				Assert.Equal("unsupported_grant_type", result["error"]);
				Assert.Contains("error_description", result.Keys);
				Assert.Contains("grant_type", result["error_description"].Value<string>());
			}
		}

		[Fact]
		public void RequestForAnotherGrantTypeShouldBeRejected()
		{
			var request = GetNewValidTokenRequest()
				.WithHeader("grant_type", "another");

			using (var server = GetNewServer())
			using (var response = request.MakeRequest())
			{
				Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

				var result = RavenJObject.Parse(response.ReadToEnd());

				Assert.Contains("error", result.Keys);
				Assert.Equal("unsupported_grant_type", result["error"]);
				Assert.Contains("error_description", result.Keys);
				Assert.Contains("grant_type", result["error_description"].Value<string>());
			}
		}

		[Fact]
		public void RequestWithoutBasicClientCredentialsShouldBeRejected()
		{
			var request = GetNewValidTokenRequest()
				.WithoutCredentials();

			using (var server = GetNewServer())
			using (var response = request.MakeRequest())
			{
				Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);

				var result = RavenJObject.Parse(response.ReadToEnd());

				Assert.Contains("error", result.Keys);
				Assert.Equal("invalid_client", result["error"]);
				Assert.Contains("error_description", result.Keys);
			}
		}

		[Fact]
		public void RequestWithInvalidClientPasswordShouldBeRejected()
		{
			var request = GetNewValidTokenRequest()
				.WithBasicCredentials(baseUrl, validClientUsername, "");

			using (var server = GetNewServer())
			using (var response = request.MakeRequest())
			{
				Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);

				var result = RavenJObject.Parse(response.ReadToEnd());

				Assert.Contains("error", result.Keys);
				Assert.Equal("unauthorized_client", result["error"]);
				Assert.Contains("error_description", result.Keys);
			}
		}
	}
}