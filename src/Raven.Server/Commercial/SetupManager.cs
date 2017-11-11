using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Certes;
using Certes.Acme;
using Certes.Pkcs;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using NetTopologySuite.Utilities;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Https;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Raven.Server.Web.Authentication;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Commercial
{
    public static class SetupManager
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        public static string SettingsPath = Path.Combine(AppContext.BaseDirectory, "settings.json");
        public const string LocalNodeTag = "A";
        public const string RavenDbDomain = "dbs.local.ravendb.net";
        public static readonly Uri LetsEncryptServer = WellKnownServers.LetsEncrypt;
        
        public static async Task<Uri> LetsEncryptAgreement(string email)
        {
            if (IsValidEmail(email) == false)
                throw new ArgumentException("Invalid e-mail format" + email);

            using (var acmeClient = new AcmeClient(LetsEncryptServer))
            {
                var account = await acmeClient.NewRegistraton("mailto:" + email);
                return account.GetTermsOfServiceUri();
            }
        }

        public static async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo, ServerStore serverStore)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 2
            };

            try
            {
                progress.AddInfo("Setting up RavenDB in 'Secured Mode'.");
                progress.AddInfo("Starting validation.");
                onProgress(progress);

                ValidateSetupInfo(SetupMode.Secured, setupInfo);

                try
                {
                    await ValidateServerCanRunWithSuppliedSettings(token, setupInfo, serverStore, SetupMode.Secured);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation failed.", e);
                }

                progress.Processed++;
                progress.AddInfo("Validation is successful.");
                progress.AddInfo("Creating new RavenDB configuration settings.");
                onProgress(progress);

                try
                {
                    progress.SettingsZipFile =
                        await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, progress, token, SetupMode.Secured, setupInfo, serverStore);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not create configuration settings.", e);
                }

                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                onProgress(progress);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in 'Secured Mode' failed.", e);
            }
            return progress;
        }

        public static async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, CancellationToken token, SetupInfo setupInfo, ServerStore serverStore)
        {
            var progress = new SetupProgressAndResult
            {
                Processed = 0,
                Total = 4
            };

            try
            {
                progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
                onProgress(progress);
                try
                {
                    ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Validation of supplied settings failed.", e);
                }


                // TODO: Check if we have matching cert cached in memory
                // If so, we can proceed without talking to LE.

                progress.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
                onProgress(progress);

                using (var acmeClient = new AcmeClient(LetsEncryptServer))
                {
                    var dictionary = new Dictionary<string, Task<Challenge>>();
                    Dictionary<string, string> map;
                    try
                    {
                        var account = await acmeClient.NewRegistraton("mailto:" + setupInfo.Email);
                        account.Data.Agreement = account.GetTermsOfServiceUri();
                        await acmeClient.UpdateRegistration(account);

                        foreach (var tag in setupInfo.NodeSetupInfos.Select(node => node.Key[0]))
                        {
                            var host = $"{tag.ToString().ToLower()}.{setupInfo.Domain.ToLower()}";
                            var fullHost = host + ".dbs.local.ravendb.net";
                            var authz = acmeClient.NewAuthorization(new AuthorizationIdentifier
                            {
                                Type = AuthorizationIdentifierTypes.Dns,
                                Value = fullHost
                            }).ContinueWith(t =>
                            {
                                return t.Result.Data.Challenges.First(c => c.Type == ChallengeTypes.Dns01);
                            }, token);
                            dictionary[tag.ToString()] = authz;
                        }

                        await Task.WhenAll(dictionary.Values.ToArray());
                        map = dictionary.ToDictionary(x => x.Key, x => acmeClient.ComputeDnsValue(x.Value.Result));
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to receive challenge(s) information from Let's Encrypt.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Successfully received challenge(s) information from Let's Encrypt.");
                    progress.AddInfo("updating DNS record(s) and challenge(s) in dbs.local.ravendb.net.");
                    onProgress(progress);

                    try
                    {
                        //TODO: Check if we already set the DNS records and we have a cached LE cert
                        // Note that the user may chose to change the internal IP / port, but that is fine
                        // As long as the cert is there, the hostnames match and the DNS records we update
                        // are the same

                        await UpdateDnsRecordsTask(onProgress, progress, token, map, setupInfo);

                        // Cache the current DNS topology so we can check it again
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to update DNS record(s) and challenge(s) in dbs.local.ravendb.net.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Successfully updated DNS record(s) and challenge(s) in dbs.local.ravendb.net.");
                    progress.AddInfo($"Completing Let's Encrypt challenge(s) for {setupInfo.Domain.ToLower()}.dbs.local.ravendb.net.");
                    onProgress(progress);

                    var csr = new CertificationRequestBuilder();
                    try
                    {
                        var tasks = new List<Task>();
                        foreach (var kvp in dictionary)
                        {
                            tasks.Add(CompleteAuthorizationFor(acmeClient, kvp.Value.Result, token));
                        }
                        await Task.WhenAll(tasks);

                        var lowerDomain = setupInfo.Domain.ToLower();
                        csr.AddName($"CN=a.{lowerDomain}.dbs.local.ravendb.net");

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            csr.SubjectAlternativeNames.Add($"{node.Key.ToLower()}.{lowerDomain}.dbs.local.ravendb.net");
                        }
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to Complete Let's Encrypt challenge(s).", e);
                    }

                    progress.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                    progress.AddInfo("Acquiring certificate.");
                    onProgress(progress);

                    AcmeCertificate cert;
                    try
                    {

                        cert = await acmeClient.NewCertificate(csr);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to aquire certificate from Let's Encrypt.", e);
                    }

                    try
                    {
                        var pfxBuilder = cert.ToPfx();
                        var certBytes = pfxBuilder.Build(setupInfo.Domain.ToLower() + " cert", "");
                        setupInfo.Certificate = Convert.ToBase64String(certBytes);

                        //TODO: Cache this (in memory)
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to build certificate from Let's Encrypt.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Successfully acquired certificate from Let's Encrypt.");
                    progress.AddInfo("Starting validation.");
                    onProgress(progress);

                    try
                    {
                        await ValidateServerCanRunWithSuppliedSettings(token, setupInfo, serverStore, SetupMode.LetsEncrypt);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Validation failed.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Validation is successful.");
                    progress.AddInfo("Creating new RavenDB configuration settings.");

                    onProgress(progress);

                    try
                    {
                        progress.SettingsZipFile =
                            await CreateSettingsZipAndOptionallyWriteToLocalServer(onProgress, progress, token, SetupMode.LetsEncrypt, setupInfo, serverStore);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to create the configuration settings.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Configuration settings created.");
                    progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode finished successfully.");
                    onProgress(progress);
                }
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in Let's Encrypt security mode failed.", e);
            }
            return progress;
        }

        private static void LogErrorAndThrow(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, string msg, Exception e)
        {
            progress.AddError(msg, e);
            onProgress.Invoke(progress);
            throw new InvalidOperationException(msg, e);
        }

        private static async Task UpdateDnsRecordsTask(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, CancellationToken token, Dictionary<string, string> map, SetupInfo setupInfo)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(15)).Token))
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    SubDomains = new List<RegistrationNodeInfo>()
                };

                foreach (var domainAndChallenge in map)
                {
                    progress.AddInfo($"Creating Dns record/challenge for node {domainAndChallenge.Key}.");
                    onProgress(progress);
                    var regNodeInfo = new RegistrationNodeInfo
                    {
                        SubDomain = domainAndChallenge.Key,
                        Challenge = domainAndChallenge.Value,
                        Ips = setupInfo.NodeSetupInfos[domainAndChallenge.Key].Addresses
                    };
                    registrationInfo.SubDomains.Add(regNodeInfo);
                }

                var serializeObject = JsonConvert.SerializeObject(registrationInfo);
                HttpResponseMessage response;
                try
                {
                    progress.AddInfo("Registering DNS record(s)/challenge(s) in api.ravendb.net.");
                    progress.AddInfo("Please wait between 30 seconds and a few minutes, depending on the number of domains(nodes).");
                    progress.AddInfo("Waiting for DNS records to update...");
                    onProgress(progress);
                    response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register",
                        new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Registration request to api.ravendb.net failed for: " + serializeObject, e);
                }

                var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                if (response.IsSuccessStatusCode == false)
                {
                    throw new InvalidOperationException(
                        $"Got unsuccessful response from registration request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                }

                var id = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseString).First().Value;

                try
                {
                    RegistrationResult registrationResult;
                    var i = 1;
                    do
                    {
                        try
                        {
                            await Task.Delay(1000, cts.Token);
                            response = await ApiHttpClient.Instance.PostAsync("/v4/dns-n-cert/registration-result?id=" + id,
                                    new StringContent(serializeObject, Encoding.UTF8, "application/json"), cts.Token)
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Registration-result request to api.ravendb.net failed.", e); //add the object we tried to send to error
                        }

                        responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                        if (response.IsSuccessStatusCode == false)
                        {
                            throw new InvalidOperationException(
                                $"Got unsuccessful response from registration-result request: {response.StatusCode}.{Environment.NewLine}{responseString}");
                        }

                        registrationResult = JsonConvert.DeserializeObject<RegistrationResult>(responseString);

                        if (i % 120 == 0)
                            progress.AddInfo("This is taking too long, you might want to abort and restart if this goes on like this...");
                        else if (i % 45 == 0)
                            progress.AddInfo("If everything goes all right, we should be nearly there...");
                        else if (i % 30 == 0)
                            progress.AddInfo("The DNS update is still pending, carry on just a little bit longer...");
                        else if (i % 15 == 0)
                            progress.AddInfo("Please be patient, updating DNS records takes time...");
                        else if (i % 5 == 0)
                            progress.AddInfo("Waiting...");
                        
                        onProgress(progress);

                        i++;
                    } while (registrationResult.Status == "PENDING");
                    progress.AddInfo("Got successful response from api.ravendb.net.");
                    onProgress(progress);
                }
                catch (Exception e)
                {
                    if (cts.IsCancellationRequested == false)
                        throw;
                    throw new TimeoutException("Request failed due to a timeout error", e);
                }
            }
        }

        private static async Task CompleteAuthorizationFor(AcmeClient client, Challenge dnsChallenge, CancellationToken token)
        {
            var challenge = await client.CompleteChallenge(dnsChallenge);

            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token))
            {
                while (true)
                {
                    if (cts.IsCancellationRequested)
                        throw new TimeoutException("Timeout expired on completion of ACME authorization");

                    var authz = await client.GetAuthorization(challenge.Location);
                    if (authz.Data.Status == EntityStatus.Pending)
                    {
                        await Task.Delay(250, cts.Token);
                        continue;
                    }

                    if (authz.Data.Status == EntityStatus.Valid)
                        return;

                    throw new InvalidOperationException("Failed to authorize certificate: " + authz.Data.Status + Environment.NewLine + authz.Json);
                }
            }
        }

        public static async Task ValidateServerCanRunWithSuppliedSettings(CancellationToken token, SetupInfo setupInfo, ServerStore serverStore, SetupMode setupMode)
        {
            var localNode = setupInfo.NodeSetupInfos[LocalNodeTag];

            var ips = localNode.Addresses.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port)).ToArray();

            X509Certificate2 serverCert;
            try
            {
                var localCertBytes = Convert.FromBase64String(setupInfo.Certificate);
                serverCert = string.IsNullOrEmpty(setupInfo.Password)
                    ? new X509Certificate2(localCertBytes)
                    : new X509Certificate2(localCertBytes, setupInfo.Password);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not load the provided certificate.", e);
            }

            var localServerUrl = GetServerUrlFromCertificate(serverCert, setupInfo, LocalNodeTag, localNode.Port, out var domain);
            setupInfo.Domain = domain;

            try
            {
                if (serverStore.Server.ListenEndpoints.Port == localNode.Port)
                {
                    var currentIps = serverStore.Server.ListenEndpoints.Addresses;
                    if (ips.Length == 0 && currentIps.Length == 1 &&
                        (Equals(currentIps[0], IPAddress.Any) || Equals(currentIps[0], IPAddress.IPv6Any)))
                        return; // listen to any ip in this 

                    if (ips.All(ip => currentIps.Contains(ip.Address)))
                        return; // we already listen to all these IPs, no need to check
                }

                await SimulateRunningServer(serverCert, localServerUrl, ips, token, setupInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + localServerUrl, e);
            }
        }

        public static void ValidateSetupInfo(SetupMode setupMode, SetupInfo setupInfo)
        {
            if (setupMode == SetupMode.LetsEncrypt)
            {
                if (setupInfo.NodeSetupInfos.ContainsKey(LocalNodeTag) == false)
                    throw new ArgumentException($"At least one of the nodes must have the node tag '{LocalNodeTag}'.");
                if (IsValidEmail(setupInfo.Email) == false)
                    throw new ArgumentException("Invalid email address.");
                if (IsValidDomain(setupInfo.Domain) == false)
                    throw new ArgumentException("Invalid domain name.");
            }

            if (setupMode == SetupMode.Secured && string.IsNullOrWhiteSpace(setupInfo.Certificate))
                throw new ArgumentException($"{nameof(setupInfo.Certificate)} is a mandatory property for a secured setup");

            foreach (var node in setupInfo.NodeSetupInfos)
            {
                if (string.IsNullOrWhiteSpace(node.Key) || node.Key.Length != 1 || !char.IsLetter(node.Key[0]) || !char.IsUpper(node.Key[0]))
                    throw new ArgumentException("Node Tag [A-Z] (capital) is a mandatory property for a secured setup");

                if (node.Value.Port == 0)
                    setupInfo.NodeSetupInfos[node.Key].Port = 443;
            }
        }

        public static bool IsValidEmail(string email)
        {
            if (string.IsNullOrWhiteSpace(email))
                return false;
            try
            {
                var addr = new System.Net.Mail.MailAddress(email);
                return addr.Address == email;
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

        public static void WriteSettingsJsonLocally(string settingsPath, string json)
        {
            var tmpPath = settingsPath + ".tmp";
            using (var file = new FileStream(tmpPath, FileMode.Create))
            using (var writer = new StreamWriter(file))
            {
                writer.Write(json);
                writer.Flush();
                file.Flush(true);
            }

            File.Replace(tmpPath, settingsPath, settingsPath + ".bak");
            if (PlatformDetails.RunningOnPosix)
                Syscall.FsyncDirectoryFor(settingsPath);
        }

        private static string GetServerUrlFromCertificate(X509Certificate2 cert, SetupInfo setupInfo, string nodeTag, int port, out string domain)
        {
            var cn = cert.GetNameInfo(X509NameType.DnsName, false);
            if (cn[0] == '*')
            {
                var parts = cn.Split("*.");
                if (parts.Length != 2)
                    throw new FormatException($"{cn} is not a valid wildcard name for a certificate.");

                domain = parts[1];

                if (port == 443)
                    return $"https://{nodeTag.ToLower()}.{domain}";

                return $"https://{nodeTag.ToLower()}.{domain}:{port}";
            }

            domain = cn; //default for one node case

            var sanNames = cert.Extensions["2.5.29.17"];
            // If we have alternative names, find the apropriate url using the node tag
            foreach (var line in sanNames.Format(true).Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries))
            {
                string[] parts;

                if (line.Contains('='))
                {
                    parts = line.Split('=');
                }
                else if (line.Contains(':'))
                {
                    parts = line.Split(':');
                }
                else
                {
                    throw new InvalidOperationException($"Could not parse SAN names: {line}");
                }

                var value = parts.Length > 0 ? parts[1] : "";
                
                if (value.StartsWith(nodeTag, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                domain = value;
                break;
            }

            var url = $"https://{domain}";
            if (port != 443)
                url += ":" + port;

            setupInfo.NodeSetupInfos[nodeTag].PublicServerUrl = url;

            return setupInfo.NodeSetupInfos[nodeTag].PublicServerUrl;
        }

        private static async Task<byte[]> CreateSettingsZipAndOptionallyWriteToLocalServer(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, CancellationToken token, SetupMode setupMode, SetupInfo setupInfo, ServerStore serverStore)
        {
            try
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var originalSettings = File.ReadAllText(SettingsPath);
                        dynamic jsonObj = JsonConvert.DeserializeObject(originalSettings);

                        progress.AddInfo("Loading and validating server certificate.");
                        onProgress(progress);
                        byte[] serverCertBytes;
                        X509Certificate2 serverCert;
                        try
                        {
                            var base64 = setupInfo.Certificate;
                            serverCertBytes = Convert.FromBase64String(base64);
                            serverCert = string.IsNullOrEmpty(setupInfo.Password)
                                ? new X509Certificate2(serverCertBytes)
                                : new X509Certificate2(serverCertBytes, setupInfo.Password);


                            var publicServerUrl =
                                GetServerUrlFromCertificate(serverCert, setupInfo, LocalNodeTag, setupInfo.NodeSetupInfos[LocalNodeTag].Port, out var domain);
                            setupInfo.Domain = domain;
                            serverStore.EnsureNotPassive(publicServerUrl);

                            serverStore.Server.ClusterCertificateHolder = //TODO: also in webhost validation
                                SecretProtection.ValidateCertificateAndCreateCertificateHolder(base64, "Setup", serverCert, serverCertBytes, setupInfo.Password);

                            if (PlatformDetails.RunningOnPosix)
                                EnsureCaExistsInOsStores(base64, "setup certificate", serverStore);

                            foreach (var node in setupInfo.NodeSetupInfos)
                            {
                                if (node.Key == LocalNodeTag)
                                    continue;
                                progress.AddInfo($"Adding node '{node.Key}' to the cluster.");
                                onProgress(progress);

                                setupInfo.NodeSetupInfos[node.Key].PublicServerUrl =
                                    GetServerUrlFromCertificate(serverCert, setupInfo, node.Key, node.Value.Port, out var _);

                                if (node.Key == LocalNodeTag)
                                    continue;

                                try
                                {
                                    await serverStore.AddNodeToClusterAsync(setupInfo.NodeSetupInfos[node.Key].PublicServerUrl, node.Key, validateNotInTopology: false);
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException($"Failed to add node '{node.Key}' to the cluster.", e);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Could not load the certificate in the local server.", e);
                        }

                        progress.AddInfo("Generating the client certificate.");
                        onProgress(progress);
                        X509Certificate2 clientCert;

                        try
                        {
                            // requires server certificate to be loaded
                            clientCert = await GenerateCertificateTask($"{setupInfo.Domain.ToLower()}.client.certificate", serverStore);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Could not generate a client certificate for '{setupInfo.Domain.ToLower()}'.", e);
                        }

                        RegisterClientCertInOs(onProgress, progress, clientCert);

                        progress.AddInfo("Writing certificates to zip archive.");
                        onProgress(progress);
                        try
                        {
                            var entry = archive.CreateEntry($"server/cluster.server.certificate.{setupInfo.Domain.ToLower()}.pfx");
                            using (var entryStream = entry.Open())
                            using (var writer = new BinaryWriter(entryStream))
                            {
                                writer.Write(serverCertBytes);
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }

                            entry = archive.CreateEntry($"admin.client.certificate.{setupInfo.Domain.ToLower()}.pfx");
                            using (var entryStream = entry.Open())
                            using (var writer = new BinaryWriter(entryStream))
                            {
                                writer.Write(clientCert.Export(X509ContentType.Pfx));
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }

                            entry = archive.CreateEntry($"admin.client.certificate.{setupInfo.Domain.ToLower()}.pem");
                            using (var entryStream = entry.Open())
                            using (var writer = new StreamWriter(entryStream))
                            {
                                var builder = new StringBuilder();
                                builder.AppendLine("-----BEGIN CERTIFICATE-----");
                                builder.AppendLine(Convert.ToBase64String(clientCert.Export(X509ContentType.Cert),
                                    Base64FormattingOptions.InsertLineBreaks)); //Todo test this. does it includes the private key
                                builder.AppendLine("-----END CERTIFICATE-----");

                                writer.Write(builder.ToString());
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write the certificates to a zip archive.", e);
                        }

                        jsonObj["Setup.Mode"] = setupMode.ToString();
                        jsonObj["Security.Certificate.Base64"] = setupInfo.Certificate;
                        if (string.IsNullOrEmpty(setupInfo.Password) == false)
                            jsonObj["Security.Certificate.Password"] = setupInfo.Password;

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            progress.AddInfo($"Creating settings file 'settings.josn' for node {node.Key}.");
                            onProgress(progress);

                            jsonObj["ServerUrl"] = string.Join(";", node.Value.Addresses.Select(address =>
                            {
                                var url = "https://" + address;
                                if (node.Value.Port != 443)
                                    url += ":" + node.Value.Port;
                                return url;
                            }));

                            if (string.IsNullOrEmpty(node.Value.PublicServerUrl))
                                jsonObj["PublicServerUrl"] = GetServerUrlFromCertificate(serverCert, setupInfo, node.Key, setupInfo.NodeSetupInfos[LocalNodeTag].Port, out var _);
                            else
                                jsonObj["PublicServerUrl"] = node.Value.PublicServerUrl;

                            if (string.IsNullOrEmpty(setupInfo.Password) == false)
                                jsonObj["Security.Certificate.Password"] = setupInfo.Password;

                            var jsonString = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

                            if (node.Key == LocalNodeTag && setupInfo.ModifyLocalServer)
                            {
                                try
                                {
                                    WriteSettingsJsonLocally(SettingsPath, jsonString);
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException("Failed to write settings file 'settings.josn' for the local sever.", e);
                                }
                            }

                            progress.AddInfo($"Adding settings file for node '{node.Key}' to zip archive.");
                            onProgress(progress);
                            try
                            {
                                var entry = archive.CreateEntry($"{node.Key}/settings.json");
                                using (var entryStream = entry.Open())
                                using (var writer = new StreamWriter(entryStream))
                                {
                                    writer.Write(jsonString);
                                    writer.Flush();
                                    await entryStream.FlushAsync(token);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($"Failed to write settings.json for node '{node.Key}' in zip archive.", e);
                            }
                        }

                        progress.AddInfo("Adding readme file to zip archive.");
                        onProgress(progress);
                        string readmeString = CreateReadmeText();
                        try
                        {
                            var entry = archive.CreateEntry("readme.txt");
                            using (var entryStream = entry.Open())
                            using (var writer = new StreamWriter(entryStream))
                            {
                                writer.Write(readmeString);
                                writer.Flush();
                                await entryStream.FlushAsync(token);
                            }

                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write readme.txt to zip archive.", e);
                        }
                    }
                    return ms.ToArray();
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create setting file(s).", e);
            }
        }

        public static void RegisterClientCertInOs(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, X509Certificate2 clientCert)
        {
            using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser, OpenFlags.ReadWrite))
            {
                try
                {
                    userPersonalStore.Add(clientCert);
                    progress.AddInfo($"Successfully registered the admin client certificate in the OS Personal CurrentUser Store '{userPersonalStore.Name}'.");
                    onProgress(progress);
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to register client certificate in the current user personal store '{userPersonalStore.Name}'.", e);
                }
            }
        }

        private static string CreateReadmeText()
        {
            //todo 
            /*
             * We need to mention that the cert is in plain text in the settings.json
              we can determine where file was saved. 
              it would be nice to explicitly tell file name -> purpose
              'You can now access your server securely' actually you have to restart server first
              'If you are setting up a cluster' I think we can detect this case. About copying settings.json file I think it is worth to mention that user should override old one - so we have implicit hint what he should look for (directory with settings.json file)
             */

            return $"Your cluster settings zip file has been downloaded to PATH. It contains the server and client certificates and a settings.json file for each node." +
                   $"\r\n\r\nYou can now access your server securely." +
                   $"\r\n\r\nIf you are using Chrome or Edge, add the client certificate to the OS trusted root store. Then access the following URL: XXXXX" +
                   $"\r\n\r\nIf you are using Firefox, the certificate must be imported directly to the browser." +
                   $"\r\n\r\nIf you are setting up a cluster with more than one node, the other nodes must be started with these new configuration settings. You must copy the settings.json file to the directory where the server is located, on each machine hosting a node." +
                   $"\r\n\r\nOnce the other nodes are started, the local node will detect it, and add them automatically to the cluster. ";
        }

        private class UniqueResponseResponder : IStartup
        {
            private readonly string _response;

            public UniqueResponseResponder(string response)
            {
                _response = response;
            }

            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
                var x = services.BuildServiceProvider();

                return services.BuildServiceProvider();
            }

            public void Configure(IApplicationBuilder app)
            {
                app.Run(async context =>
                {
                    await context.Response.WriteAsync(_response);
                });
            }
        }

        public static async Task SimulateRunningServer(X509Certificate2 serverCertificate, string serverUrl, IPEndPoint[] addresses, CancellationToken token, SetupInfo setupInfo)
        {
            var configuration = new RavenConfiguration(null, ResourceType.Server, SettingsPath);
            configuration.Initialize();
            var guid = Guid.NewGuid().ToString();

            IWebHost webHost = null;
            try
            {
                try
                {
                    var responder = new UniqueResponseResponder(guid);

                    webHost = new WebHostBuilder()
                        .CaptureStartupErrors(captureStartupErrors: true)
                        .UseKestrel(options =>
                        {
                            var port = setupInfo.NodeSetupInfos[LocalNodeTag].Port;
                            if (addresses.Length == 0)
                            {
                                var defaultIp = new IPEndPoint(IPAddress.Parse("0.0.0.0"), port == 0 ? 443 : port);
                                options.Listen(defaultIp, 
                                    listenOptions => listenOptions.ConnectionAdapters.Add(new HttpsConnectionAdapter(serverCertificate)));
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"List of ip addresses for node '{LocalNodeTag}' is empty. Webhost listening to {defaultIp}");
                            }

                            foreach (var addr in addresses)
                            {
                                options.Listen(addr,
                                    listenOptions => listenOptions.ConnectionAdapters.Add(new HttpsConnectionAdapter(serverCertificate)));
                            }
                        })
                        .UseSetting(WebHostDefaults.ApplicationKey, "Setup simulation")
                        .ConfigureServices(collection =>
                        {
                            collection.AddSingleton(typeof(IStartup), responder);
                        })
                        .UseShutdownTimeout(TimeSpan.FromMilliseconds(150))
                        .Build();

                    webHost.Start();
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to start webhost on node '{LocalNodeTag}'.{Environment.NewLine}" +
                                                        $"Settings file:{SettingsPath}.{Environment.NewLine} " +
                                                        $"IP addresses: {string.Join(", ", addresses.Select(addr => addr.ToString()))}.", e);
                }


                using (var httpMessageHandler = new HttpClientHandler())
                {
                    // on MacOS this is not supported because Apple...
                    if (PlatformDetails.RunningOnMacOsx == false)
                    {
                        httpMessageHandler.ServerCertificateCustomValidationCallback += (message, certificate2, chain, errors) =>
                        // we want to verify that we get the same thing back
                        {
                            if (certificate2.Thumbprint != serverCertificate.Thumbprint)
                                throw new InvalidOperationException("Expected to get " + serverCertificate.FriendlyName + " with thumbprint " +
                                                                    serverCertificate.Thumbprint + " but got " +
                                                                    certificate2.FriendlyName + " with thumbprint " + certificate2.Thumbprint);
                            return true;
                        };
                    }
                    using (var client = new HttpClient(httpMessageHandler)
                    {
                        BaseAddress = new Uri(serverUrl),
                    })
                    {
                        HttpResponseMessage response = null;
                        string result = null;
                        try
                        {
                            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
                            {
                                response = await client.GetAsync("/are-you-there?", cts.Token);
                                response.EnsureSuccessStatusCode();
                                result = await response.Content.ReadAsStringAsync();
                                if (result != guid)
                                {
                                    throw new InvalidOperationException($"Expected result guid:{guid} but got {result}.");
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Client failed to contact webhost listening to '{serverUrl}'.{Environment.NewLine}" +
                                                                $"Settings file:{SettingsPath}.{Environment.NewLine}" +
                                                                $"IP addresses: {string.Join(", ", addresses.Select(addr => addr.ToString()))}.{Environment.NewLine}" +
                                                                $"Response: {response?.StatusCode}.{Environment.NewLine}{result}", e);
                        }
                    }
                }
            }
            finally
            {
                if (webHost != null)
                    await webHost.StopAsync(TimeSpan.Zero);
            }
        }

        // Duplicate of AdminCertificatesHandler.GenerateCertificateInternal stripped from authz checks, used by an unauthenticated client during setup only
        public static async Task<X509Certificate2> GenerateCertificateTask(string name, ServerStore serverStore)
        {
            if (serverStore.Server.ClusterCertificateHolder?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{name}' becuase the server certificate is not loaded.");
            
            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(name, serverStore.Server.ClusterCertificateHolder);

            var newCertDef = new CertificateDefinition
            {
                Name = name,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = selfSignedCertificate.Thumbprint
            };

            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint, newCertDef));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            return selfSignedCertificate;
        }

        // Duplicate of AdminCertificatesHandler.ValidateCaExistsInOsStores but this one adds the CA if it doesn't exist.
        public static void EnsureCaExistsInOsStores(string base64Cert, string name, ServerStore serverStore)
        {
            var x509Certificate2 = new X509Certificate2(Convert.FromBase64String(base64Cert));

            var chain = new X509Chain
            {
                ChainPolicy =
                    {
                        RevocationMode = X509RevocationMode.NoCheck,
                        RevocationFlag = X509RevocationFlag.ExcludeRoot,
                        VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority,
                        VerificationTime = DateTime.UtcNow,
                        UrlRetrievalTimeout = new TimeSpan(0, 0, 0)
                    }
            };

            if (chain.Build(x509Certificate2) == false)
            {
                var status = new StringBuilder();
                if (chain.ChainStatus.Length != 0)
                {
                    status.Append("Chain Status:\r\n");
                    foreach (var chainStatus in chain.ChainStatus)
                        status.Append(chainStatus.Status + " : " + chainStatus.StatusInformation + "\r\n");
                }

                throw new InvalidOperationException($"The certificate chain for {name} is broken, admin assistance required. {status}");
            }

            var rootCert = AdminCertificatesHandler.GetRootCertificate(chain);
            if (rootCert == null)
                throw new InvalidOperationException($"The certificate chain for {name} is broken. Reason: partial chain, cannot extract CA from chain. Admin assistance required.");


            using (var machineRootStore = new X509Store(StoreName.Root, StoreLocation.LocalMachine, OpenFlags.ReadWrite))
            using (var machineCaStore = new X509Store(StoreName.CertificateAuthority, StoreLocation.LocalMachine, OpenFlags.ReadWrite))
            using (var userRootStore = new X509Store(StoreName.Root, StoreLocation.CurrentUser, OpenFlags.ReadOnly))
            using (var userCaStore = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser, OpenFlags.ReadOnly))
            {
                // workaround for lack of cert store inheritance RavenDB-8904
                if (machineCaStore.Certificates.Contains(rootCert) == false
                    && machineRootStore.Certificates.Contains(rootCert) == false
                    && userCaStore.Certificates.Contains(rootCert) == false
                    && userRootStore.Certificates.Contains(rootCert) == false)
                {
                    // rootCert is not in the stores, if we're in docker we have permissions, so lets add the cert
                    if (Environment.GetEnvironmentVariable("RAVEN_AUTO_INSTALL_CA") == "true")
                    {
                        try
                        {
                            machineRootStore.Add(rootCert);
                            userRootStore.Add(rootCert);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("The CA of the self signed certificate you're trying to use is not trusted by the OS.\r\n " +
                                                                "Tried to register the CA in the trusted root store, but failed (permissions?).\r\n" +
                                                                "You need to do this manually and restart the setup.\r\n" +
                                                                "Please read the Linux setup section in the documentation.\r\n", e);
                        }
                    }

                    throw new InvalidOperationException("The CA of the self signed certificate you're trying to use is not trusted by the OS.\r\n" +
                                                        "You need to do this manually and restart the setup.\r\n" +
                                                        "Please read the Linux setup section in the documentation.\r\n");
                }
            }
        }
    }
}
