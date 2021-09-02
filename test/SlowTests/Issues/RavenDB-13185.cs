using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13185 : ReplicationTestBase
    {
        public RavenDB_13185(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanHandleCorsHeaders()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var (_, leader, certificates) = await CreateRaftClusterWithSsl(clusterSize, false);

            X509Certificate2 adminCertificate = RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);

            var members = leader.ServerStore.GetClusterTopology().Members.Values.ToList();
            var nonLeaderUrl = members.First(x => x != leader.WebUrl);
            var leaderUrl = leader.WebUrl;
            var externalUrl = "http://example.com";

            foreach (var certToUse in new List<X509Certificate2> { null, adminCertificate })
            {
                // endpoint with public Cors access - it should respond 
                {
                    // empty origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/setup/alive", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.False(response.Headers.Contains("Access-Control-Allow-Origin"));
                    }, certificate: certToUse);

                    // leader as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/setup/alive", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.True(response.Headers.Contains("Access-Control-Allow-Origin"));
                        Assert.Equal(leaderUrl, response.Headers.GetValues("Access-Control-Allow-Origin").FirstOrDefault());
                    }, leaderUrl, certToUse);

                    // member as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/setup/alive", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.True(response.Headers.Contains("Access-Control-Allow-Origin"));
                        Assert.Equal(nonLeaderUrl, response.Headers.GetValues("Access-Control-Allow-Origin").FirstOrDefault());
                    }, nonLeaderUrl, certToUse);

                    // random as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/setup/alive", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.True(response.Headers.Contains("Access-Control-Allow-Origin"));
                        Assert.Equal(externalUrl, response.Headers.GetValues("Access-Control-Allow-Origin").FirstOrDefault());
                    }, externalUrl, certToUse);
                }

                { // endpoint with cluster wide CORS policy

                    // empty origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/cluster/demote", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.False(response.Headers.Contains("Access-Control-Allow-Origin"));
                    }, certificate: certToUse);

                    // leader as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/cluster/demote", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.True(response.Headers.Contains("Access-Control-Allow-Origin"));
                        Assert.Equal(leaderUrl, response.Headers.GetValues("Access-Control-Allow-Origin").FirstOrDefault());
                    }, leaderUrl, certToUse);

                    // member as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/cluster/demote", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.True(response.Headers.Contains("Access-Control-Allow-Origin"));
                        Assert.Equal(nonLeaderUrl, response.Headers.GetValues("Access-Control-Allow-Origin").FirstOrDefault());
                    }, nonLeaderUrl, certToUse);

                    // random as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/cluster/demote", response =>
                    {
                        Assert.True(response.IsSuccessStatusCode);
                        Assert.False(response.Headers.Contains("Access-Control-Allow-Origin"));
                    }, externalUrl, certToUse);

                }

                { // endpoint with out CORS policy

                    // empty origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/databases", response =>
                    {
                        Assert.False(response.IsSuccessStatusCode);
                    }, certificate: certToUse);

                    // leader as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/databases", response =>
                    {
                        Assert.False(response.IsSuccessStatusCode);
                    }, leaderUrl, certToUse);

                    // member as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/databases", response =>
                    {
                        Assert.False(response.IsSuccessStatusCode);
                    }, nonLeaderUrl, certToUse);

                    // random as origin
                    await ExecuteRequest(HttpMethod.Options, leaderUrl + "/admin/databases", response =>
                    {
                        Assert.False(response.IsSuccessStatusCode);
                    }, externalUrl, certToUse);
                }
            }
        }

        private async Task ExecuteRequest(HttpMethod method, string uri, Action<HttpResponseMessage> assertion, string origin = null, X509Certificate2 certificate = null)
        {
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (message, certificate2, arg3, arg4) => true,
                SslProtocols = TcpUtils.SupportedSslProtocols
            };

            if (certificate != null)
            {
                handler.ClientCertificates.Add(certificate);
            }

            using (var httpClient = new HttpClient(handler))
            {
                HttpRequestMessage request = new HttpRequestMessage
                {
                    Method = method,
                    RequestUri = new Uri(uri)
                };

                if (origin != null)
                {
                    request.Headers.Add("Origin", origin);
                }

                using (var response = await httpClient.SendAsync(request))
                {
                    assertion(response);
                }
            }
        }
    }
}
