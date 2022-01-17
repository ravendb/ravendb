using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Commercial.LetsEncrypt;

public class LetsEncryptValidationApiHelper
{
    private static async Task AssertLocalNodeCanListenToEndpoints(SetupInfo setupInfo, ServerStore serverStore)
    {
        var localNode = setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag];
        var localIps = new List<IPEndPoint>();

        // Because we can get from user either an ip or a hostname, we resolve the hostname and get the actual ips it is mapped to
        foreach (var hostnameOrIp in localNode.Addresses)
        {
            if (hostnameOrIp.Equals("0.0.0.0"))
            {
                localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.Port));
                localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.TcpPort));
                continue;
            }

            foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
            {
                localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.Port));
                localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.TcpPort));
            }
        }

        var requestedEndpoints = localIps.ToArray();
        var currentServerEndpoints = serverStore.Server.ListenEndpoints.Addresses.Select(ip => new IPEndPoint(ip, serverStore.Server.ListenEndpoints.Port)).ToList();

        var ipProperties = IPGlobalProperties.GetIPGlobalProperties();
        IPEndPoint[] activeTcpListeners;
        try
        {
            activeTcpListeners = ipProperties.GetActiveTcpListeners();
        }
        catch (Exception)
        {
            // If GetActiveTcpListeners is not supported, skip the validation
            // See https://github.com/dotnet/corefx/issues/30909
            return;
        }

        foreach (var requestedEndpoint in requestedEndpoints)
        {
            if (activeTcpListeners.Contains(requestedEndpoint))
            {
                if (currentServerEndpoints.Contains(requestedEndpoint))
                    continue; // OK... used by the current server

                throw new InvalidOperationException(
                    $"The requested endpoint '{requestedEndpoint.Address}:{requestedEndpoint.Port}' is already in use by another process. You may go back in the wizard, change the settings and try again.");
            }
        }
    }

    internal static async Task ValidateServerCanRunWithSuppliedSettings(SetupInfo setupInfo, ServerStore serverStore, SetupMode setupMode, CancellationToken token)
    {
        var localNode = setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag];
        var localIps = new List<IPEndPoint>();

        foreach (var hostnameOrIp in localNode.Addresses)
        {
            if (hostnameOrIp.Equals("0.0.0.0"))
            {
                localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.Port));
                continue;
            }

            foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
            {
                localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.Port));
            }
        }

        var serverCert = setupInfo.GetX509Certificate();

        var localServerUrl =
            LetsEncryptCertificateUtil.GetServerUrlFromCertificate(serverCert, setupInfo, setupInfo.LocalNodeTag, localNode.Port, localNode.TcpPort, out _, out _);

        try
        {
            if (serverStore.Server.ListenEndpoints.Port == localNode.Port)
            {
                var currentIps = serverStore.Server.ListenEndpoints.Addresses.ToList();

                if (localIps.Count == 0 && currentIps.Count == 1 &&
                    (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                    return; // listen to any ip in this

                if (localIps.All(ip => currentIps.Contains(ip.Address)))
                    return; // we already listen to all these IPs, no need to check
            }

            if (setupMode == SetupMode.LetsEncrypt)
            {
                // In case an external ip was specified, this is the ip we update in the dns records. (not the one we bind to)
                var ips = localNode.ExternalIpAddress == null
                    ? localIps.ToArray()
                    : new[] {new IPEndPoint(IPAddress.Parse(localNode.ExternalIpAddress), localNode.ExternalPort)};

                await RavenDnsRecordHelper.AssertDnsUpdatedSuccessfully(localServerUrl, ips, token);
            }

            // Here we send the actual ips we will bind to in the local machine.
            await ServerSimulationHelper.SimulateRunningServer(serverStore, serverCert, localServerUrl, setupInfo.LocalNodeTag, localIps.ToArray(), localNode.Port,
                serverStore.Configuration.ConfigPath, setupMode, token);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + localServerUrl, e);
        }
    }

    internal static async Task ValidateServerCanRunOnThisNode(BlittableJsonReaderObject settingsJsonObject, X509Certificate2 cert, ServerStore serverStore,
        string nodeTag, CancellationToken token)
    {
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

        var serverUrls = serverUrl.Split(";");
        var port = new Uri(serverUrls[0]).Port;
        var hostnamesOrIps = serverUrls.Select(url => new Uri(url).DnsSafeHost).ToArray();

        var localIps = new List<IPEndPoint>();

        foreach (var hostnameOrIp in hostnamesOrIps)
        {
            if (hostnameOrIp.Equals("0.0.0.0"))
            {
                localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), port));
                continue;
            }

            foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp))
            {
                localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), port));
            }
        }

        try
        {
            if (serverStore.Server.ListenEndpoints.Port == port)
            {
                var currentIps = serverStore.Server.ListenEndpoints.Addresses.ToList();

                if (localIps.Count == 0 && currentIps.Count == 1 &&
                    (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                    return; // listen to any ip in this

                if (localIps.All(ip => currentIps.Contains(ip.Address)))
                    return; // we already listen to all these IPs, no need to check
            }

            if (setupMode == SetupMode.LetsEncrypt)
            {
                var ips = string.IsNullOrEmpty(externalIp)
                    ? localIps.ToArray()
                    : new[] {new IPEndPoint(IPAddress.Parse(externalIp), port)};

                await RavenDnsRecordHelper.AssertDnsUpdatedSuccessfully(publicServerUrl, ips, token);
            }

            // Here we send the actual ips we will bind to in the local machine.
            await ServerSimulationHelper.SimulateRunningServer(serverStore, cert, publicServerUrl, nodeTag, localIps.ToArray(), port, serverStore.Configuration.ConfigPath, setupMode,
                token);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + publicServerUrl, e);
        }
    }

    internal static async Task ValidateSetupInfo(SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore)
    {
        if ((await SetupParameters.Get(serverStore)).IsDocker)
        {
            if (setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag].Addresses.Any(ip => ip.StartsWith("127.")))
            {
                throw new InvalidOperationException("When the server is running in Docker, you cannot bind to ip 127.X.X.X, please use the hostname instead.");
            }
        }

        switch (setupMode)
        {
            case SetupMode.LetsEncrypt when setupInfo.NodeSetupInfos.ContainsKey(setupInfo.LocalNodeTag) == false:
                throw new ArgumentException($"At least one of the nodes must have the node tag '{setupInfo.LocalNodeTag}'.");
            case SetupMode.LetsEncrypt when IsValidEmail(setupInfo.Email) == false:
                throw new ArgumentException("Invalid email address.");
            case SetupMode.LetsEncrypt when IsValidDomain(setupInfo.Domain + "." + setupInfo.RootDomain) == false:
                throw new ArgumentException("Invalid domain name.");
            case SetupMode.LetsEncrypt when setupInfo.ClientCertNotAfter.HasValue && setupInfo.ClientCertNotAfter <= DateTime.UtcNow.Date:
                throw new ArgumentException("The client certificate expiration date must be in the future.");
            case SetupMode.Secured when string.IsNullOrWhiteSpace(setupInfo.Certificate):
                throw new ArgumentException($"{nameof(setupInfo.Certificate)} is a mandatory property for a secured setup");
        }

        foreach ((string key, SetupInfo.NodeInfo value) in setupInfo.NodeSetupInfos)
        {
            RachisConsensus.ValidateNodeTag(key);

            if (value.Port == 0)
                setupInfo.NodeSetupInfos[key].Port = 443;

            if (value.TcpPort == 0)
                setupInfo.NodeSetupInfos[key].TcpPort = 38888;

            if (setupMode == SetupMode.LetsEncrypt &&
                setupInfo.NodeSetupInfos[key].Addresses.Any(ip => ip.Equals("0.0.0.0")) &&
                string.IsNullOrWhiteSpace(setupInfo.NodeSetupInfos[key].ExternalIpAddress))
            {
                throw new ArgumentException("When choosing 0.0.0.0 as the ip address, you must provide an external ip to update in the DNS records.");
            }
        }

        await AssertLocalNodeCanListenToEndpoints(setupInfo, serverStore);
    }

    public static bool IsValidEmail(string email)
    {
        if (string.IsNullOrWhiteSpace(email))
            return false;
        try
        {
            var address = new System.Net.Mail.MailAddress(email);
            return address.Address == email;
        }
        catch
        {
            return false;
        }
    }

    private static bool IsValidDomain(string domain)
    {
        if (string.IsNullOrWhiteSpace(domain))
            return false;

        return Uri.CheckHostName(domain) != UriHostNameType.Unknown;
    }
}
