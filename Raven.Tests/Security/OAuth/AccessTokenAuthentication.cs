using System;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Security.OAuth
{

    public class AccessTokenAuthentication : RemoteClientTest, IDisposable
    {
        readonly string path;
        readonly string publicKeyPath;
        readonly string privateKeyPath;
        const string relativeUrl = "/raven/studio.html";
        const string baseUrl = "http://localhost";
        const int port = 8080;

        public AccessTokenAuthentication()
        {
            path = GetPath("TestDb");
            publicKeyPath = GetPath(@"Security\OAuth\Public.cer");
            privateKeyPath = GetPath(@"Security\OAuth\Private.pfx");
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
        }

        protected override void ConfigureServer(Database.Config.RavenConfiguration ravenConfiguration)
        {
            ravenConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
            ravenConfiguration.AuthenticationMode = "OAuth";
            ravenConfiguration.OAuthTokenCertificatePath = publicKeyPath;
        }

        public void Dispose()
        {
            IOExtensions.DeleteDirectory(path);
        }
        
        public string GetAccessToken(string user = "jsmith", string databases = "*", bool valid = true, bool expired = false)
        {
            var issued = (DateTime.UtcNow - DateTime.MinValue).TotalMilliseconds;

            if (expired) issued -= TimeSpan.FromHours(1).TotalMilliseconds;

            var body = RavenJObject.FromObject(new { UserId = user, AuthorizedDatabases = databases.Split(','), Issued = issued }).ToString(Formatting.None);

            var signature = valid ? CertHelper.Sign(body, privateKeyPath) : "InvalidSignature";
            
            var token = RavenJObject.FromObject(new { Body = body, Signature = signature }).ToString(Formatting.None);

            return token;
        }

        static HttpWebRequest GetNewWebRequest()
        {
            return ((HttpWebRequest)WebRequest.Create(baseUrl + ":" + port + relativeUrl));
        }

        [Fact]
        public void RequestsWithAValidAccessTokenShouldBeAccepted()
        {
            var token = GetAccessToken();

            var request = GetNewWebRequest()
                .WithBearerTokenAuthorization(token);

            using (var server = GetNewServer(false))
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            }
        }
        
        [Fact]
        public void RequestsWithoutAnAccessTokenShouldBeRejected()
        {
            var request = GetNewWebRequest();

            using (var server = GetNewServer(false))
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
                var challenge = response.Headers["WWW-Authenticate"];
                Assert.NotEmpty(challenge);
                Assert.True(challenge.StartsWith("Bearer "));
                Assert.Contains("error=\"invalid_request\"", challenge);
            }
        }

        [Fact]
        public void RequestsWithAnInvalidAccessTokenShouldBeRejected()
        {
            var token = GetAccessToken(valid:false);

            var request = GetNewWebRequest()
                .WithBearerTokenAuthorization(token);

            using (var server = GetNewServer(false))
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
                var challenge = response.Headers["WWW-Authenticate"];
                Assert.NotEmpty(challenge);
                Assert.True(challenge.StartsWith("Bearer"));
                Assert.Contains("error=\"invalid_token\"", challenge);
            }
        }

        [Fact]
        public void RequestsWithAnExpiredAccessTokenShouldBeRejected()
        {
            var token = GetAccessToken(expired: true);

            var request = GetNewWebRequest()
                .WithBearerTokenAuthorization(token);

            using (var server = GetNewServer(false))
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
                var challenge = response.Headers["WWW-Authenticate"];
                Assert.NotEmpty(challenge);
                Assert.True(challenge.StartsWith("Bearer"));
                Assert.Contains("error=\"invalid_token\"", challenge);
            }
        }

        [Fact]
        public void RequestsWithAnAccessTokenThatDoesNotHaveTheNeededScopeShouldBeRejected()
        {
            var token = GetAccessToken(databases : "");

            var request = GetNewWebRequest()
                .WithBearerTokenAuthorization(token);

            using (var server = GetNewServer(false))
            using (var response = request.MakeRequest())
            {
                Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
                var challenge = response.Headers["WWW-Authenticate"];
                Assert.NotEmpty(challenge);
                Assert.True(challenge.StartsWith("Bearer"));
                Assert.Contains("error=\"insufficient_scope\"", challenge);
            }
        }
    }
}