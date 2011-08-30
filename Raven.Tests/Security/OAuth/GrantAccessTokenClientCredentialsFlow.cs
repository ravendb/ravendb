using System;
using System.ComponentModel.Composition.Hosting;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Http.Security.OAuth;
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
		const int port = 8080;
		const string validClientUsername = "client1";
		const string validClientPassword = "password";

		public GrantAccessTokenClientCredentialsFlow()
		{
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);

		}

		protected override void ConfigureServer(Database.Config.RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			ravenConfiguration.AuthenticationMode = "OAuth";
			ravenConfiguration.OAuthTokenCertificate = CertGenerator.GenerateNewCertificate("RavenDB.Test");
			ravenConfiguration.Catalog.Catalogs.Add(new TypeCatalog(typeof(FakeAuthenticateClient)));
		}

		public class FakeAuthenticateClient : IAuthenticateClient
		{
			public bool Authenticate(IResourceStore currentStore, string username, string password, out string[] allowedDatabases)
			{
				allowedDatabases = new[] {"*"};
				return string.IsNullOrEmpty(password) == false;
			}
		}

		public void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
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

			using (var server = GetNewServer(false))
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

			using (var server = GetNewServer(false))
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

			using (var server = GetNewServer(false))
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

			using (var server = GetNewServer(false))
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

			using (var server = GetNewServer(false))
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

			using (var server = GetNewServer(false))
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