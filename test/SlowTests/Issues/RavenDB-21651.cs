using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server;
using Xunit.Abstractions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_21651 : ReplicationTestBase
{
    public RavenDB_21651(ITestOutputHelper output) : base(output)
    {
    }

    public const string ExternalTrustedOriginHostname = "external-trusted-origin";
    public const string ExternalTrustedOriginUrl = "http://external-trusted-origin:8080";
    public const string OriginHeader = "X-Forwarded-Host";

    public const string ProxyServerHost = "proxy:5656";
    public const string ProxyServerUrl = "http://proxy:5656";

    public const string EvilOrigin = "http://hacked-server:8080";

    [RavenFact(RavenTestCategory.Studio | RavenTestCategory.Security)]
    public async Task CsrfProtectionForUnsecuredSingleNodeServerBaseCase()
    {
        // we are using default CSRF settings
        var (_, leader) = await CreateRaftCluster(1, false);

        var testUrl = leader.WebUrl + "/databases";
        var leaderHost = new Uri(leader.WebUrl).Authority;
        var sameHostAsLeaderButDifferentPort = "http://" + new Uri(leader.WebUrl).Host + ":21";

        await Act(testUrl, leaderHost, sameHostAsLeaderButDifferentPort, leader);
    }
    
    [RavenFact(RavenTestCategory.Studio | RavenTestCategory.Security)]
    public async Task CsrfProtectionForUnsecuredSingleNodeServer_WithoutCsrf()
    {
        var settings = new Dictionary<string, string>
        {
            {"Security.Csrf.Enabled", "false"}
        };
        var (_, leader) = await CreateRaftCluster(1, false, customSettings: settings);

        var testUrl = leader.WebUrl + "/databases";
        var leaderHost = new Uri(leader.WebUrl).Authority;
        var sameHostAsLeaderButDifferentPort = "http://" + new Uri(leader.WebUrl).Host + ":21";

        await Act(testUrl, leaderHost, sameHostAsLeaderButDifferentPort, leader);
    }

    [RavenFact(RavenTestCategory.Studio | RavenTestCategory.Security)]
    public async Task CsrfProtectionForUnsecuredSingleNodeServer()
    {
        var settings = new Dictionary<string, string>
        {
            {"Security.Csrf.TrustedOrigins", ExternalTrustedOriginHostname}, {"Security.Csrf.AdditionalOriginHeaders", OriginHeader}
        };

        var (_, leader) = await CreateRaftCluster(1, false, customSettings: settings);

        var testUrl = leader.WebUrl + "/databases";
        var leaderHost = new Uri(leader.WebUrl).Authority;
        var sameHostAsLeaderButDifferentPort = "http://" + new Uri(leader.WebUrl).Host + ":21";

        await Act(testUrl, leaderHost, sameHostAsLeaderButDifferentPort, leader);
    }

    [RavenFact(RavenTestCategory.Studio | RavenTestCategory.Security)]
    public async Task CsrfProtectionForSecuredCluster()
    {
        var clusterSize = 3;
        var databaseName = GetDatabaseName();
        var (_, leader, certificates) = await CreateRaftClusterWithSsl(clusterSize, false);

        X509Certificate2 adminCertificate =
            Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: leader);

        var testUrl = leader.WebUrl + "/databases";
        var leaderHost = new Uri(leader.WebUrl).Authority;
        var sameHostAsLeaderButDifferentPort = "http://" + new Uri(leader.WebUrl).Host + ":21";

        await Act(testUrl, leaderHost, sameHostAsLeaderButDifferentPort, leader, adminCertificate);
    }

    private async Task Act(string testUrl, string host, string sameHostAsLeaderButDifferentPort, RavenServer server, X509Certificate2 certificate = null)
    {
        bool csrfEnabled = server.Configuration.Security.EnableCsrfFilter;
        bool acceptProxy = !csrfEnabled || server.Configuration.Security.CsrfAdditionalOriginHeaders?.Length > 0;
        bool acceptAllowedOrigin = !csrfEnabled || server.Configuration.Security.CsrfTrustedOrigins?.Length > 0;

        var nodes = server.ServerStore.GetClusterTopology().AllNodes.Values.ToList();
        var differentNode = nodes.FirstOrDefault(x => !x.Contains(host));
        
        // no CSRF for OPTIONS
        var clusterObserverDecisionsUrl = server.WebUrl + "/admin/cluster/observer/decisions";

        await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl, new Dictionary<string, string>(), true, certificate);
        await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl, new Dictionary<string, string> {{"Host", host}}, true, certificate);
        await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", EvilOrigin}}, true, certificate);
        await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl,
            new Dictionary<string, string> {{"Host", host}, {"Origin", sameHostAsLeaderButDifferentPort}}, true, certificate);
        await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", ExternalTrustedOriginUrl}},
            true, certificate);
        await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl,
            new Dictionary<string, string> {{"Host", host}, {"Origin", ProxyServerUrl}, {OriginHeader, ProxyServerHost}}, true, certificate);
        await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl,
            new Dictionary<string, string> {{"Host", ProxyServerHost}, {"Origin", EvilOrigin}, {OriginHeader, server.WebUrl}}, true, certificate);

        await ExecuteRequest(HttpMethod.Get, testUrl, new Dictionary<string, string>(), true, certificate);
        await ExecuteRequest(HttpMethod.Get, testUrl, new Dictionary<string, string> {{"Host", host}}, true, certificate);
        await ExecuteRequest(HttpMethod.Get, testUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", EvilOrigin}}, !csrfEnabled, certificate);
        await ExecuteRequest(HttpMethod.Get, testUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", sameHostAsLeaderButDifferentPort}}, !csrfEnabled,
            certificate);
        await ExecuteRequest(HttpMethod.Get, testUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", ExternalTrustedOriginUrl}}, acceptAllowedOrigin,
            certificate);
        await ExecuteRequest(HttpMethod.Get, testUrl,
            new Dictionary<string, string> {{"Host", host}, {"Origin", ProxyServerUrl}, {OriginHeader, ProxyServerHost}}, acceptProxy, certificate);
        await ExecuteRequest(HttpMethod.Get, testUrl, new Dictionary<string, string> {{"Host", ProxyServerHost}, {"Origin", EvilOrigin}, {OriginHeader, server.WebUrl}},
            !csrfEnabled, certificate);

        var eulaUrl = server.WebUrl + "/admin/license/eula/accept";
        await ExecuteRequest(HttpMethod.Post, eulaUrl, new Dictionary<string, string>(), true, certificate);
        await ExecuteRequest(HttpMethod.Post, eulaUrl, new Dictionary<string, string> {{"Host", host}}, true, certificate);
        await ExecuteRequest(HttpMethod.Post, eulaUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", EvilOrigin}}, !csrfEnabled, certificate);
        await ExecuteRequest(HttpMethod.Post, eulaUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", sameHostAsLeaderButDifferentPort}}, !csrfEnabled,
            certificate);
        await ExecuteRequest(HttpMethod.Post, eulaUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", ExternalTrustedOriginUrl}}, acceptAllowedOrigin,
            certificate);
        await ExecuteRequest(HttpMethod.Post, eulaUrl,
            new Dictionary<string, string> {{"Host", host}, {"Origin", ProxyServerUrl}, {OriginHeader, ProxyServerHost}}, acceptProxy, certificate);
        await ExecuteRequest(HttpMethod.Post, eulaUrl, new Dictionary<string, string> {{"Host", ProxyServerHost}, {"Origin", EvilOrigin}, {OriginHeader, server.WebUrl}},
            !csrfEnabled, certificate);
        
        if (differentNode != null)
        {
            // cross-cluster
            await ExecuteRequest(HttpMethod.Options, clusterObserverDecisionsUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", differentNode}}, true, certificate);
            await ExecuteRequest(HttpMethod.Get, testUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", differentNode}}, true, certificate);
            await ExecuteRequest(HttpMethod.Post, eulaUrl, new Dictionary<string, string> {{"Host", host}, {"Origin", differentNode}}, true, certificate);
        }
    }


    private async Task ExecuteRequest(HttpMethod method, string uri, Dictionary<string, string> headers, bool allowed, X509Certificate2 certificate = null)
    {
        var handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true, 
            SslProtocols = TcpUtils.SupportedSslProtocols,
            AllowAutoRedirect = true
        };

        if (certificate != null)
        {
            handler.ClientCertificates.Add(certificate);
        }

        using (var httpClient = new HttpClient(handler))
        {
            HttpRequestMessage request = new() {Method = method, RequestUri = new Uri(uri)};

            if (headers != null)
            {
                foreach ((string key, string value) in headers)
                {
                    request.Headers.Add(key, value);
                }
            }

            using (var response = await httpClient.SendAsync(request))
            {
                if (allowed)
                {
                    Assert.True(response.IsSuccessStatusCode, "Expected successful response but got: " + response.StatusCode);
                }
                else
                {
                    Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
                }
            }
        }
    }
}
