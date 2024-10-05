using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Http;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Commercial.LetsEncrypt;

public sealed class RavenDnsRecordHelper
{
    private const string GoogleDnsApi = "https://dns.google.com";

    public static async Task UpdateDnsRecordsTask(UpdateDnsRecordParameters parameters)
    {
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(parameters.Token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
        {
            var registrationInfo = new RegistrationInfo
            {
                License = parameters.SetupInfo.License,
                Domain = parameters.SetupInfo.Domain,
                Challenge = parameters.Challenge,
                RootDomain = parameters.SetupInfo.RootDomain,
                SubDomains = new List<RegistrationNodeInfo>()
            };

            foreach (var node in parameters.SetupInfo.NodeSetupInfos)
            {
                var regNodeInfo = new RegistrationNodeInfo
                {
                    SubDomain = (node.Key + "." + parameters.SetupInfo.Domain).ToLower(),
                    Ips = node.Value.ExternalIpAddress == null
                        ? node.Value.Addresses
                        : new List<string> { node.Value.ExternalIpAddress }
                };

                if (parameters.RegisterTcpDnsRecords)
                {
                    var regNodeTcpInfo = new RegistrationNodeInfo
                    {
                        SubDomain = (node.Key + "-tcp." + parameters.SetupInfo.Domain).ToLower(),
                        Ips = node.Value.ExternalIpAddress == null
                            ? node.Value.Addresses
                            : new List<string> { node.Value.ExternalIpAddress }
                    };
                    registrationInfo.SubDomains.Add(regNodeTcpInfo);
                }

                registrationInfo.SubDomains.Add(regNodeInfo);
            }

            parameters.Progress?.AddInfo($"Creating DNS record/challenge for node(s): {string.Join(", ", parameters.SetupInfo.NodeSetupInfos.Keys)}.");
            parameters.OnProgress?.Invoke(parameters.Progress);

            if (registrationInfo.SubDomains.Count == 0 && registrationInfo.Challenge == null)
            {
                // no need to update anything, can skip doing DNS update
                parameters.Progress?.AddInfo("Cached DNS values matched, skipping DNS update");
                return;
            }

            var serializeObject = JsonConvert.SerializeObject(registrationInfo);
            HttpResponseMessage response;
            try
            {
                parameters.Progress?.AddInfo("Registering DNS record(s)/challenge(s) in api.ravendb.net.");
                parameters.Progress?.AddInfo("Please wait between 30 seconds and a few minutes.");
                parameters.OnProgress?.Invoke(parameters.Progress);

                response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register",
                    new StringContent(serializeObject, Encoding.UTF8, "application/json"), parameters.Token).ConfigureAwait(false);

                parameters.Progress?.AddInfo("Waiting for DNS records to update...");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
            }

            var responseString = await response.Content.ReadAsStringWithZstdSupportAsync(cts.Token).ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                throw new InvalidOperationException(
                    $"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
            }

            if (parameters.Challenge == null)
            {
                var existingSubDomain = registrationInfo.SubDomains.FirstOrDefault(x => x.SubDomain.StartsWith(parameters.SetupInfo.LocalNodeTag + ".", StringComparison.OrdinalIgnoreCase));

                if (existingSubDomain != null && new HashSet<string>(existingSubDomain.Ips).SetEquals(parameters.SetupInfo.NodeSetupInfos[parameters.SetupInfo.LocalNodeTag].Addresses))
                {
                    parameters.Progress?.AddInfo("DNS update started successfully, since current node (" + parameters.SetupInfo.LocalNodeTag + ") DNS record didn't change, not waiting for full DNS propagation.");
                    return;
                }
            }

            var id = (JsonConvert.DeserializeObject<Dictionary<string, string>>(responseString) ?? throw new InvalidOperationException()).First().Value;

            try
            {
                RegistrationResult registrationResult;
                var i = 1;
                do
                {
                    try
                    {
                        await Task.Delay(1000, cts.Token);
                        response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/registration-result?id=" + id, new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Registration-result request to api.ravendb.net failed.", e); //add the object we tried to send to error
                    }

                    responseString = await response.Content.ReadAsStringWithZstdSupportAsync(cts.Token).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode == false)
                    {
                        throw new InvalidOperationException($"Got unsuccessful response from registration-result request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                    }

                    registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(responseString);
                    if (i % 120 == 0)
                        parameters.Progress?.AddInfo("This is taking too long, you might want to abort and restart if this goes on like this...");
                    else if (i % 45 == 0)
                        parameters.Progress?.AddInfo("If everything goes all right, we should be nearly there...");
                    else if (i % 30 == 0)
                        parameters.Progress?.AddInfo("The DNS update is still pending, carry on just a little bit longer...");
                    else if (i % 15 == 0)
                        parameters.Progress?.AddInfo("Please be patient, updating DNS records takes time...");
                    else if (i % 5 == 0)
                        parameters.Progress?.AddInfo("Waiting...");

                    parameters.OnProgress?.Invoke(parameters.Progress);

                    i++;
                } while (registrationResult?.Status == "PENDING");

                parameters.Progress?.AddInfo("Got successful response from api.ravendb.net.");
                parameters.OnProgress?.Invoke(parameters.Progress);
            }
            catch (Exception e)
            {
                if (cts.IsCancellationRequested == false)
                    throw;
                throw new TimeoutException("Request failed due to a timeout error", e);
            }
        }
    }

    public static async Task AssertDnsUpdatedSuccessfully(string serverUrl, IPEndPoint[] expectedAddresses, CancellationToken token)
    {
        // First we'll try to resolve the hostname through google's public dns api
        using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
        {
            var expectedIps = expectedAddresses.Select(address => address.Address.ToString()).ToHashSet();

            var hostname = new Uri(serverUrl).Host;

            using (var client = new RavenHttpClient { BaseAddress = new Uri(GoogleDnsApi) })
            {
                var response = await client.GetAsync($"/resolve?name={hostname}", cts.Token);

                var responseString = await response.Content.ReadAsStringWithZstdSupportAsync(cts.Token).ConfigureAwait(false);
                if (response.IsSuccessStatusCode == false)
                    throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}" +
                                                               $"Request failed with status {response.StatusCode}.{Environment.NewLine}{responseString}");

                dynamic dnsResult = JsonConvert.DeserializeObject(responseString);

                // DNS response format: https://developers.google.com/speed/public-dns/docs/dns-over-https

                if (dnsResult?.Status != 0)
                    throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}" +
                                                               $"Got a DNS failure response:{Environment.NewLine}{responseString}" +
                                                               Environment.NewLine +
                                                               "Please wait a while until DNS propagation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");

                JArray answers = dnsResult.Answer;
                var googleIps = answers.Select(answer => answer["data"].ToString()).ToHashSet();

                if (googleIps.SetEquals(expectedIps) == false)
                    throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}" +
                                                               $"Expected to get these ips: {string.Join(", ", expectedIps)} while Google's actual result was: {string.Join(", ", googleIps)}" +
                                                               Environment.NewLine +
                                                               "Please wait a while until DNS propagation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");
            }

            // Resolving through google worked, now let's check locally
            HashSet<string> actualIps;
            try
            {
                actualIps = (await Dns.GetHostAddressesAsync(hostname, AddressFamily.InterNetwork, cts.Token)).Select(address => address.ToString()).ToHashSet();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot resolve '{hostname}' locally but succeeded resolving the address using Google's api ({GoogleDnsApi})." +
                                                           Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again." +
                                                           Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).", e);
            }

            if (expectedIps.SetEquals(actualIps) == false)
                throw new InvalidOperationException($"Tried to resolve '{hostname}' locally but got an outdated result." +
                                                    Environment.NewLine + $"Expected to get these ips: {string.Join(", ", expectedIps)} while the actual result was: {string.Join(", ", actualIps)}" +
                                                    Environment.NewLine + $"If we try resolving through Google's api ({GoogleDnsApi}), it works well." +
                                                    Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again." +
                                                    Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).");
        }
    }

    public static async Task<bool> CanResolveHostNameLocally(string serverUrl, IPEndPoint[] expectedAddresses)
    {
        var expectedIps = expectedAddresses.Select(address => address.Address.ToString()).ToHashSet();
        var hostname = new Uri(serverUrl).Host;
        HashSet<string> actualIps;

        try
        {
            actualIps = (await Dns.GetHostAddressesAsync(hostname)).Select(address => address.ToString()).ToHashSet();
        }
        catch (Exception)
        {
            return false;
        }

        return expectedIps.SetEquals(actualIps);
    }

    public static async Task UpdateDnsRecordsForCertificateRefreshTask(string challenge, SetupInfo setupInfo, RavenLogger logger, CancellationToken token)
    {
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
        {
            var registrationInfo = new RegistrationInfo
            {
                License = setupInfo.License,
                Domain = setupInfo.Domain,
                Challenge = challenge,
                RootDomain = setupInfo.RootDomain,
                SubDomains = new List<RegistrationNodeInfo>()
            };

            foreach (var node in setupInfo.NodeSetupInfos)
            {
                var regNodeInfo = new RegistrationNodeInfo { SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(), };

                registrationInfo.SubDomains.Add(regNodeInfo);
            }

            var serializeObject = JsonConvert.SerializeObject(registrationInfo);

            if (logger is { IsInfoEnabled: true })
                logger.Info($"Start update process for certificate. License Id: {registrationInfo.License.Id}, " +
                                      $"License Name: {registrationInfo.License.Name}, " +
                                      $"Domain: {registrationInfo.Domain}, " +
                                      $"RootDomain: {registrationInfo.RootDomain}");

            HttpResponseMessage response;
            try
            {
                response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register", new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
            }

            var responseString = await response.Content.ReadAsStringWithZstdSupportAsync(cts.Token).ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                throw new InvalidOperationException($"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
            }

            var id = (JsonConvert.DeserializeObject<Dictionary<string, string>>(responseString) ?? throw new InvalidOperationException()).First().Value;

            try
            {
                RegistrationResult registrationResult;
                do
                {
                    try
                    {
                        await Task.Delay(1000, cts.Token);
                        response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/registration-result?id=" + id, new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Registration-result request to api.ravendb.net failed.", e); //add the object we tried to send to error
                    }

                    responseString = await response.Content.ReadAsStringWithZstdSupportAsync(cts.Token).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode == false)
                    {
                        throw new InvalidOperationException($"Got unsuccessful response from registration-result request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                    }

                    registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(responseString);
                } while (registrationResult?.Status == "PENDING");
            }
            catch (Exception e)
            {
                if (cts.IsCancellationRequested == false)
                    throw;
                throw new TimeoutException("Request failed due to a timeout error", e);
            }
        }
    }
}
