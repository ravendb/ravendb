using System;
using System.Net;
using Raven.Database.Extensions;
using Raven.Http;
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
            ravenConfiguration.AuthenticationMode = "OAuth";
        }

        public void Dispose()
        {
            IOExtensions.DeleteDirectory(path);
        }

        public HttpWebRequest GetNewValidTokenRequest()
        {
            var request = ((HttpWebRequest)WebRequest.Create(baseUrl + ":" + port + tokenUrl))
                .WithBasicCredentials(baseUrl, validClientUsername, validClientPassword)
                .WithConentType("application/json;charset=UTF-8")
                .WithHeader("grant_type", "client_credentials");

            return request;
        }

        [Fact]
        public void ValidAndAuthorizedRequestShouldBeGrantedAnAccessToken()
        {
            string token;
            Guid tokenGuid;

            var request = GetNewValidTokenRequest();

            using (var server = GetNewServer())
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);

                token = response.ReadToEnd();
            }
            
            Assert.NotEmpty(token);
            Assert.True(Guid.TryParse(token, out tokenGuid));
        }

        [Fact]
        public void RequestWithoutUrlEncodedContentTypeShouldBeRejected()
        {
            var request = GetNewValidTokenRequest()
                .WithConentType("text/plain");

            using (var server = GetNewServer())
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

                var result = RavenJObject.Parse(response.ReadToEnd());

                Assert.Contains("error", result.Keys);
                Assert.Equal("invalid_request", result["error"]);
                Assert.Contains("error_description", result.Keys);
                Assert.Contains("Content-Type", result["error_description"].Value<string>());
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

                var result = RavenJObject.Parse(response.ReadToEnd());

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
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

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
                .WithBasicCredentials(baseUrl, validClientUsername, "badPass");

            using (var server = GetNewServer())
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

                var result = RavenJObject.Parse(response.ReadToEnd());

                Assert.Contains("error", result.Keys);
                Assert.Equal("unauthorized_client", result["error"]);
                Assert.Contains("error_description", result.Keys);
            }
        }
    }
}