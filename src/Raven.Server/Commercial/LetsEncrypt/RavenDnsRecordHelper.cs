using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Sparrow.Utils;

namespace Raven.Server.Commercial.LetsEncrypt;

public static class RavenDnsRecordHelper
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

                response = await ApiHttpClient.PostAsync("api/v2/dns-n-cert/register",
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
                var sw = Stopwatch.StartNew();
                do
                {
                    try
                    {
                        await TimeoutManager.WaitFor(TimeSpan.FromSeconds(5), cts.Token);
                        response = await ApiHttpClient.PostAsync($"api/v2/dns-n-cert/registration-result?id={id}", new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token).ConfigureAwait(false);
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
                    switch (sw.Elapsed.TotalSeconds)
                    {
                        case >= 120:
                        parameters.Progress?.AddInfo("This is taking too long, you might want to abort and restart if this goes on like this...");
                            break;
                        case >= 45:
                        parameters.Progress?.AddInfo("If everything goes all right, we should be nearly there...");
                            break;
                        case >= 30:
                        parameters.Progress?.AddInfo("The DNS update is still pending, carry on just a little bit longer...");
                            break;
                        case >= 15:
                        parameters.Progress?.AddInfo("Please be patient, updating DNS records takes time...");
                            break;
                        case >= 5:
                        parameters.Progress?.AddInfo("Waiting...");
                            break;
                    }

                    parameters.OnProgress?.Invoke(parameters.Progress);

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
        var hostname = new Uri(serverUrl).Host;
        
        using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
        {
            if (OnlyIpv4Addresses(expectedAddresses))
            {
                await AssertIpv4DnsUpdated(cts);
                return;
            }
            
            if (OnlyIpv6Addresses(expectedAddresses))
            {
                await AssertIpv6DnsUpdated(cts);
                return;
            }
            
            if (BothIpv4AndIpv6Addresses(expectedAddresses))
            {
                await AssertIpv4AndIpv6DnsUpdated(cts);
                return;
            }

            var unsupportedAddresses = expectedAddresses
                .Where(x => x.AddressFamily is not (AddressFamily.InterNetwork or AddressFamily.InterNetworkV6))
                .Select(x => x.AddressFamily.ToString())
                .ToList();
            
            throw new InvalidOperationException($"Tried to resolve hostname {hostname}, but encountered unsupported address types: {string.Join(", ", unsupportedAddresses)}.");
        }
        
        
        bool OnlyIpv4Addresses(IPEndPoint[] addresses) => addresses.All(x => x.AddressFamily == AddressFamily.InterNetwork);
        bool OnlyIpv6Addresses(IPEndPoint[] addresses) => addresses.All(x => x.AddressFamily == AddressFamily.InterNetworkV6);

        bool BothIpv4AndIpv6Addresses(IPEndPoint[] addresses)
        {
            var iPv4OrIpv6 = addresses.All(x => x.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6);
            var containsIpv4 = addresses.Any(x => x.AddressFamily == AddressFamily.InterNetwork);
            var containsIpv6 = addresses.Any(x => x.AddressFamily == AddressFamily.InterNetworkV6);
            
            return iPv4OrIpv6 && containsIpv4 && containsIpv6;
        }
        
        async Task AssertIpv4DnsUpdated(CancellationTokenSource cts)
        {
            var googleDnsIps = await GetGoogleDnsResult(DnsRecordType.A, cts.Token);
            AssertExpectedIpsEqualGoogleIps(googleDnsIps);
            
            var localDnsIps = await GetLocalDnsResult(AddressFamily.InterNetwork, cts.Token);
            AssertExpectedIpsEqualLocalIps(localDnsIps);
        }

        async Task AssertIpv6DnsUpdated(CancellationTokenSource cts)
        {
            var googleDnsIps = await GetGoogleDnsResult(DnsRecordType.AAAA, cts.Token);
            AssertExpectedIpsEqualGoogleIps(googleDnsIps);
            
            var localDnsIps = await GetLocalDnsResult(AddressFamily.InterNetworkV6, cts.Token);
            AssertExpectedIpsEqualLocalIps(localDnsIps);
        }

        async Task AssertIpv4AndIpv6DnsUpdated(CancellationTokenSource cts)
        {
            HashSet<string> allGoogleIps;
            try
            {
                var task1 = GetGoogleDnsResult(DnsRecordType.A, cts.Token);
                var task2 = GetGoogleDnsResult(DnsRecordType.AAAA, cts.Token);
            
                var result = await Task.WhenAll(task1,task2);
                allGoogleIps = result[0].Concat(result[1]).ToHashSet();
            }
            catch
            {
                await cts.CancelAsync();
                throw;
            }
            
            AssertExpectedIpsEqualGoogleIps(allGoogleIps);
            
            HashSet<string> allLocalIps;
            try
            {
                var task1 = GetLocalDnsResult(AddressFamily.InterNetwork, cts.Token);
                var task2 = GetLocalDnsResult(AddressFamily.InterNetworkV6, cts.Token);
                var result = await Task.WhenAll(task1, task2);
                allLocalIps = result[0].Concat(result[1]).ToHashSet();
            }
            catch
            {
               await cts.CancelAsync();
               throw;
            }

            AssertExpectedIpsEqualLocalIps(allLocalIps);
        }

        void AssertExpectedIpsEqualGoogleIps(HashSet<string> actualGoogleIps)
        {
            var allExpectedIps = GetAllExpectedIps();
            
            if (actualGoogleIps.SetEquals(allExpectedIps) == false)
                throw new InvalidOperationException($"Tried to resolve '{hostname}' using Google's api ({GoogleDnsApi}).{Environment.NewLine}" +
                                                    $"Expected to get these ips: {string.Join(", ", allExpectedIps)} while Google's actual result was: {string.Join(", ", actualGoogleIps)}" +
                                                    Environment.NewLine +
                                                    "Please wait a while until DNS propagation is finished and try again. If you are trying to update existing DNS records, it might take hours to update because of DNS caching. If the issue persists, contact RavenDB's support.");
        }

        void AssertExpectedIpsEqualLocalIps(HashSet<string> actualLocalIps)
        {
            var allExpectedIps = GetAllExpectedIps();
            
            if (allExpectedIps.SetEquals(actualLocalIps) == false)
                throw new InvalidOperationException($"Tried to resolve '{hostname}' locally but got an outdated result." +
                                                    Environment.NewLine + $"Expected to get these ips: {string.Join(", ", allExpectedIps)} while the actual result was: {string.Join(", ", actualLocalIps)}" +
                                                    Environment.NewLine + $"If we try resolving through Google's api ({GoogleDnsApi}), it works well." +
                                                    Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again." +
                                                    Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).");
        }
        
        HashSet<string> GetAllExpectedIps()
        {
            var expectedIpsV4 = expectedAddresses
                .Where(x => x.Address.AddressFamily == AddressFamily.InterNetwork)
                .Select(address => address.Address.ToString())
                .ToHashSet();
            var expectedIpsV6 = expectedAddresses
                .Where(x => x.Address.AddressFamily == AddressFamily.InterNetworkV6)
                .Select(address => address.Address.ToString())
                .ToHashSet();
            
            return expectedIpsV4.Concat(expectedIpsV6).ToHashSet();
        }

        async Task<HashSet<string>> GetGoogleDnsResult(DnsRecordType recordType, CancellationToken ct)
        {
            using (var client = new RavenHttpClient { BaseAddress = new Uri(GoogleDnsApi) })
            {
                var response = await client.GetAsync($"/resolve?name={hostname}&type={recordType}", ct);
                var responseString = await response.Content.ReadAsStringWithZstdSupportAsync(ct).ConfigureAwait(false);
                
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
                return answers.Select(answer => answer["data"].ToString()).ToHashSet();
            }
        }

        async Task<HashSet<string>> GetLocalDnsResult(AddressFamily addressFamily, CancellationToken ct)
        {
            try
            {
                var result = (await Dns.GetHostAddressesAsync(hostname, addressFamily, ct)).Select(address => address.ToString()).ToHashSet();
                return result;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot resolve '{hostname}' locally but succeeded resolving the address using Google's api ({GoogleDnsApi})." +
                                                    Environment.NewLine + "Try to clear your local/network DNS cache or wait a few minutes and try again." +
                                                    Environment.NewLine + "Another temporary solution is to configure your local network connection to use Google's DNS server (8.8.8.8).", e);
            }       
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

    public static async Task UpdateDnsRecordsForCertificateRefreshTask(string challenge, SetupInfo setupInfo, Logger logger, CancellationToken token)
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

            if (logger is { IsOperationsEnabled: true })
                logger.Operations($"Start update process for certificate. License Id: {registrationInfo.License.Id}, " +
                                      $"License Name: {registrationInfo.License.Name}, " +
                                      $"Domain: {registrationInfo.Domain}, " +
                                      $"RootDomain: {registrationInfo.RootDomain}");

            HttpResponseMessage response;
            try
            {
                response = await ApiHttpClient.PostAsync("api/v2/dns-n-cert/register", new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
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
                        await TimeoutManager.WaitFor(TimeSpan.FromSeconds(5), cts.Token);
                        response = await ApiHttpClient.PostAsync($"api/v2/dns-n-cert/registration-result?id={id}", new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token).ConfigureAwait(false);
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

    private enum DnsRecordType
    {
        A = 1,
        AAAA = 28
    }
}
