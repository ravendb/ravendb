using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Platform.Posix;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Commercial
{
    public static class LetsEncryptUtils
    {
        public static async Task<byte[]> SetupLetsEncryptTask(SetupInfo setupInfo, CancellationToken token)
        {
            try
            {
                var acmeClient = new LetsEncryptClient("https://acme-v02.api.letsencrypt.org/directory");
                await acmeClient.Init(setupInfo.Email, token);
                var challengeResult = await InitialLetsEncryptChallenge(setupInfo, acmeClient, token);
                await UpdateDnsRecordsTask((_) => { }, null, challengeResult.Challenge, setupInfo, token);
                await CompleteAuthorizationAndGetCertificate(() => { }, setupInfo, acmeClient, challengeResult, token);
                using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
                {
                    return await CompleteClusterConfigurationAndGetSettingsZip(context, SetupMode.LetsEncrypt, setupInfo, token);
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Setting up RavenDB in Let's Encrypt security mode failed", e);
            }
        }

        public static string IpAddressToUrl(string address, int port)
        {
            var url = "https://" + address;
            if (port != 443)
                url += ":" + port;
            return url;
        }

        public static string IpAddressToTcpUrl(string address, int port)
        {
            var url = "tcp://" + address;
            if (port != 0)
                url += ":" + port;
            return url;
        }

        internal static string GetServerUrlFromCertificate(X509Certificate2 cert, SetupInfo setupInfo, string nodeTag, int port, int tcpPort, out string publicTcpUrl,
            out string domain)
        {
            publicTcpUrl = null;
            var node = setupInfo.NodeSetupInfos[nodeTag];

            var cn = cert.GetNameInfo(X509NameType.SimpleName, false);
            if (cn[0] == '*')
            {
                var parts = cn.Split("*.");
                if (parts.Length != 2)
                    throw new FormatException($"{cn} is not a valid wildcard name for a certificate.");

                domain = parts[1];

                publicTcpUrl = node.ExternalTcpPort != 0
                    ? $"tcp://{nodeTag.ToLower()}.{domain}:{node.ExternalTcpPort}"
                    : $"tcp://{nodeTag.ToLower()}.{domain}:{tcpPort}";

                if (setupInfo.NodeSetupInfos[nodeTag].ExternalPort != 0)
                    return $"https://{nodeTag.ToLower()}.{domain}:{node.ExternalPort}";

                return port == 443
                    ? $"https://{nodeTag.ToLower()}.{domain}"
                    : $"https://{nodeTag.ToLower()}.{domain}:{port}";
            }

            domain = cn; //default for one node case

            foreach (var value in GetCertificateAlternativeNames(cert))
            {
                if (value.StartsWith(nodeTag + ".", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                domain = value;
                break;
            }

            var url = $"https://{domain}";

            if (node.ExternalPort != 0)
                url += ":" + node.ExternalPort;
            else if (port != 443)
                url += ":" + port;

            publicTcpUrl = node.ExternalTcpPort != 0
                ? $"tcp://{domain}:{node.ExternalTcpPort}"
                : $"tcp://{domain}:{tcpPort}";

            node.PublicServerUrl = url;
            node.PublicTcpServerUrl = publicTcpUrl;

            return url;
        }

        public static IEnumerable<string> GetCertificateAlternativeNames(X509Certificate2 cert)
        {
            // If we have alternative names, find the appropriate url using the node tag
            var sanNames = cert.Extensions["2.5.29.17"];

            if (sanNames == null)
                yield break;

            var generalNames = GeneralNames.GetInstance(Asn1Object.FromByteArray(sanNames.RawData));

            foreach (var certHost in generalNames.GetNames())
            {
                yield return certHost.Name.ToString();
            }
        }

        internal static async Task<byte[]> CompleteClusterConfigurationAndGetSettingsZip(JsonOperationContext context, SetupMode setupMode, SetupInfo setupInfo,
            CancellationToken token)
        {
            try
            {
                // var settingsPath = serverStore.Configuration.ConfigPath;
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        byte[] serverCertBytes;
                        RavenServer.CertificateHolder serverCert = new();
                        string domainFromCert;
                        string publicServerUrl;

                        try
                        {
                            var base64 = setupInfo.Certificate;
                            serverCertBytes = Convert.FromBase64String(base64);
                            serverCert.Certificate = new X509Certificate2(serverCertBytes, setupInfo.Password, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
                            
                            var localNodeTag = setupInfo.LocalNodeTag;
                            publicServerUrl = GetServerUrlFromCertificate(serverCert.Certificate, setupInfo, localNodeTag, setupInfo.NodeSetupInfos[localNodeTag].Port,
                                setupInfo.NodeSetupInfos[localNodeTag].TcpPort, out _, out domainFromCert);

                            foreach (var node in setupInfo.NodeSetupInfos)
                            {
                                if (node.Key == setupInfo.LocalNodeTag)
                                    continue;

                                setupInfo.NodeSetupInfos[node.Key].PublicServerUrl = GetServerUrlFromCertificate(serverCert.Certificate, setupInfo, node.Key, node.Value.Port,
                                    node.Value.TcpPort, out _, out _);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Could not load the certificate in the local server.", e);
                        }

                        X509Certificate2 clientCert;

                        var name = (setupMode == SetupMode.Secured)
                            ? domainFromCert.ToLower()
                            : setupInfo.Domain.ToLower();

                        byte[] certBytes;
                        try
                        {
                            // requires server certificate to be loaded
                            var clientCertificateName = $"{name}.client.certificate";
                           (byte[] bytes, CertificateDefinition certificateDefinition, string item3) = await GenerateCertificateTask(clientCertificateName, serverCert, setupInfo);
                            clientCert = new X509Certificate2(bytes, (string)null,
                                X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
                            certBytes = bytes;
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Could not generate a client certificate for '{name}'.", e);
                        }

                        // if (setupInfo.RegisterClientCert)
                        //     RegisterClientCertInOs(onProgress, progress, clientCert);

                        try
                        {
                            var entry = archive.CreateEntry($"admin.client.certificate.{name}.pfx");

                            // Structure of external attributes field: https://unix.stackexchange.com/questions/14705/the-zip-formats-external-file-attribute/14727#14727
                            // The permissions go into the most significant 16 bits of an int
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                            await using (var entryStream = entry.Open())
                            {
                                var export = clientCert.Export(X509ContentType.Pfx);
                                await entryStream.WriteAsync(export, 0, export.Length, token);
                            }

                            await WriteCertificateAsPemAsync($"admin.client.certificate.{name}", certBytes, null, archive);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write the certificates to a zip archive.", e);
                        }

                        BlittableJsonReaderObject settingsJson;
                        await using (var fs = SafeFileStream.Create("settingsPath", FileMode.Open, FileAccess.Read))
                        {
                            settingsJson = await context.ReadForMemoryAsync(fs, "settings-json");
                        }

                        var data = new DynamicJsonValue {[""] = "Value"};

                        if (setupMode == SetupMode.LetsEncrypt)
                        {
                            try
                            {
                                var licenseString = JsonConvert.SerializeObject(setupInfo.License, Formatting.Indented);

                                var entry = archive.CreateEntry("license.json");
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                await using (var entryStream = entry.Open())
                                await using (var writer = new StreamWriter(entryStream))
                                {
                                    await writer.WriteAsync(licenseString);
                                    await writer.FlushAsync();
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException("Failed to write license.json in zip archive.", e);
                            }
                        }

                        settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString();

                        if (setupInfo.EnableExperimentalFeatures)
                        {
                            settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = FeaturesAvailability.Experimental;
                        }

                        var certificateFileName = $"cluster.server.certificate.{name}.pfx";
                        var certPath = Path.Combine(AppContext.BaseDirectory, certificateFileName);

                        if (setupInfo.ModifyLocalServer)
                        {
                            await using (var certFile = SafeFileStream.Create(certPath, FileMode.Create))
                            {
                                await certFile.WriteAsync(serverCertBytes, 0, serverCertBytes.Length, token);
                                await certFile.FlushAsync(token);
                            } // we'll be flushing the directory when we'll write the settings.json
                        }
//x.Security.CertificatePath
// x.Security.CertificatePassword
                        settingsJson.Modifications[RavenConfiguration.GetKey(x =>"" )] = certPath;
                        if (string.IsNullOrEmpty(setupInfo.Password) == false)
                            settingsJson.Modifications[RavenConfiguration.GetKey(x =>"")] = setupInfo.Password;

                        foreach (var node in setupInfo.NodeSetupInfos)
                        {
                            var currentNodeSettingsJson = settingsJson.Clone(context);
                            currentNodeSettingsJson.Modifications = currentNodeSettingsJson.Modifications ?? new DynamicJsonValue(currentNodeSettingsJson);

                            if (node.Value.Addresses.Count != 0)
                            {
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] =
                                    string.Join(";", node.Value.Addresses.Select(ip => IpAddressToUrl(ip, node.Value.Port)));
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] =
                                    string.Join(";", node.Value.Addresses.Select(ip => IpAddressToTcpUrl(ip, node.Value.TcpPort)));
                            }

                            var httpUrl = GetServerUrlFromCertificate(serverCert.Certificate, setupInfo, node.Key, node.Value.Port,
                                node.Value.TcpPort, out var tcpUrl, out var _);

                            if (string.IsNullOrEmpty(node.Value.ExternalIpAddress) == false)
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = node.Value.ExternalIpAddress;

                            currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] =
                                string.IsNullOrEmpty(node.Value.PublicServerUrl)
                                    ? httpUrl
                                    : node.Value.PublicServerUrl;

                            currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] =
                                string.IsNullOrEmpty(node.Value.PublicTcpServerUrl)
                                    ? tcpUrl
                                    : node.Value.PublicTcpServerUrl;

                            var modifiedJsonObj = context.ReadObject(currentNodeSettingsJson, "modified-settings-json");

                            var indentedJson = IndentJsonString(modifiedJsonObj.ToString());
                            // if (node.Key == setupInfo.LocalNodeTag && setupInfo.ModifyLocalServer)
                            // {
                            //     try
                            //     {
                            //         WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson);
                            //     }
                            //     catch (Exception e)
                            //     {
                            //         throw new InvalidOperationException("Failed to write settings file 'settings.json' for the local sever.", e);
                            //     }
                            // }
                            //
                            try
                            {
                                var entry = archive.CreateEntry($"{node.Key}/settings.json");
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                await using (var entryStream = entry.Open())
                                await using (var writer = new StreamWriter(entryStream))
                                {
                                    await writer.WriteAsync(indentedJson);
                                    await writer.FlushAsync();
                                }

                                // we save this multiple times on each node, to make it easier
                                // to deploy by just copying the node
                                entry = archive.CreateEntry($"{node.Key}/{certificateFileName}");
                                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                                await using (var entryStream = entry.Open())
                                {
                                    await entryStream.WriteAsync(serverCertBytes, 0, serverCertBytes.Length);
                                }
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($"Failed to write settings.json for node '{node.Key}' in zip archive.", e);
                            }
                        }

                        try
                        {
                            var settings = new SetupSettings {Nodes = setupInfo.NodeSetupInfos.Select(tag => new SetupSettings.Node {Tag = tag.Key}).ToArray()};

                            var modifiedJsonObj = context.ReadObject(settings.ToJson(), "setup-json");

                            var indentedJson = IndentJsonString(modifiedJsonObj.ToString());

                            var entry = archive.CreateEntry("setup.json");
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                            await using (var entryStream = entry.Open())
                            await using (var writer = new StreamWriter(entryStream))
                            {
                                await writer.WriteAsync(indentedJson);
                                await writer.FlushAsync();
                                await entryStream.FlushAsync(token);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write setup.json to zip archive.", e);
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

        public static async Task WriteCertificateAsPemAsync(string name, byte[] rawBytes, string exportPassword, ZipArchive archive)
        {
            var a = new Pkcs12Store();
            a.Load(new MemoryStream(rawBytes), Array.Empty<char>());

            X509CertificateEntry entry = null;
            AsymmetricKeyEntry key = null;
            foreach (var alias in a.Aliases)
            {
                var aliasKey = a.GetKey(alias.ToString());
                if (aliasKey != null)
                {
                    entry = a.GetCertificate(alias.ToString());
                    key = aliasKey;
                    break;
                }
            }

            if (entry == null)
            {
                throw new InvalidOperationException("Could not find private key.");
            }

            var zipEntryCrt = archive.CreateEntry(name + ".crt");
            zipEntryCrt.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            await using (var stream = zipEntryCrt.Open())
            await using (var writer = new StreamWriter(stream))
            {
                var pw = new PemWriter(writer);
                pw.WriteObject(entry.Certificate);
            }

            var zipEntryKey = archive.CreateEntry(name + ".key");
            zipEntryKey.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            await using (var stream = zipEntryKey.Open())
            await using (var writer = new StreamWriter(stream))
            {
                var pw = new PemWriter(writer);

                object privateKey;
                if (exportPassword != null)
                {
                    privateKey = new MiscPemGenerator(
                            key.Key,
                            "DES-EDE3-CBC",
                            exportPassword.ToCharArray(),
                            CertificateUtils.GetSeededSecureRandom())
                        .Generate();
                }
                else
                {
                    privateKey = key.Key;
                }

                pw.WriteObject(privateKey);

                await writer.FlushAsync();
            }
        }


        internal static async Task<(string Challenge, LetsEncryptClient.CachedCertificateResult Cache)> InitialLetsEncryptChallenge(
            SetupInfo setupInfo,
            LetsEncryptClient client,
            CancellationToken token)
        {
            try
            {
                var host = (setupInfo.Domain + "." + setupInfo.RootDomain).ToLowerInvariant();
                var wildcardHost = "*." + host;
                if (client.TryGetCachedCertificate(wildcardHost, out var certBytes))
                    return (null, certBytes);

                var result = await client.NewOrder(new[] {wildcardHost}, token);

                result.TryGetValue(host, out var challenge);
                // we may already be authorized for this?
                return (challenge, null);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to receive challenge(s) information from Let's Encrypt.", e);
            }
        }

        private static X509Certificate2 BuildNewPfx(SetupInfo setupInfo, X509Certificate2 certificate, RSA privateKey)
        {
            var certWithKey = certificate.CopyWithPrivateKey(privateKey);

            Pkcs12Store store = new Pkcs12StoreBuilder().Build();

            var chain = new X509Chain();
            chain.ChainPolicy.DisableCertificateDownloads = true;

            chain.Build(certificate);

            foreach (var item in chain.ChainElements)
            {
                var x509Certificate = DotNetUtilities.FromX509Certificate(item.Certificate);

                if (item.Certificate.Thumbprint == certificate.Thumbprint)
                {
                    var key = new AsymmetricKeyEntry(DotNetUtilities.GetKeyPair(certWithKey.PrivateKey).Private);
                    store.SetKeyEntry(x509Certificate.SubjectDN.ToString(), key, new[] {new X509CertificateEntry(x509Certificate)});
                    continue;
                }

                store.SetCertificateEntry(item.Certificate.Subject, new X509CertificateEntry(x509Certificate));
            }

            var memoryStream = new MemoryStream();
            store.Save(memoryStream, Array.Empty<char>(), new SecureRandom(new CryptoApiRandomGenerator()));
            var certBytes = memoryStream.ToArray();

            Debug.Assert(certBytes != null);
            setupInfo.Certificate = Convert.ToBase64String(certBytes);

            return new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
        }

        internal static async Task<X509Certificate2> CompleteAuthorizationAndGetCertificate(Action onValidationSuccessful, SetupInfo setupInfo, LetsEncryptClient client,
            (string Challange, LetsEncryptClient.CachedCertificateResult Cache) challengeResult, CancellationToken token)
        {
            if (challengeResult.Challange == null && challengeResult.Cache != null)
            {
                return BuildNewPfx(setupInfo, challengeResult.Cache.Certificate, challengeResult.Cache.PrivateKey);
            }

            try
            {
                await client.CompleteChallenges(token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to Complete Let's Encrypt challenge(s).", e);
            }

            onValidationSuccessful();

            (X509Certificate2 Cert, RSA PrivateKey) result;
            try
            {
                result = await client.GetCertificate(null, token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to acquire certificate from Let's Encrypt.", e);
            }

            try
            {
                return BuildNewPfx(setupInfo, result.Cert, result.PrivateKey);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to build certificate from Let's Encrypt.", e);
            }
        }

        private static async Task UpdateDnsRecordsTask(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            string challenge,
            SetupInfo setupInfo,
            CancellationToken token)
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
                    var regNodeInfo = new RegistrationNodeInfo
                    {
                        SubDomain = (node.Key + "." + setupInfo.Domain).ToLower(),
                        Ips = node.Value.ExternalIpAddress == null
                            ? node.Value.Addresses
                            : new List<string> {node.Value.ExternalIpAddress}
                    };

                    registrationInfo.SubDomains.Add(regNodeInfo);
                }

                progress.AddInfo($"Creating DNS record/challenge for node(s): {string.Join(", ", setupInfo.NodeSetupInfos.Keys)}.");

                onProgress(progress);

                if (registrationInfo.SubDomains.Count == 0 && registrationInfo.Challenge == null)
                {
                    // no need to update anything, can skip doing DNS update
                    progress.AddInfo("Cached DNS values matched, skipping DNS update");
                    return;
                }

                var serializeObject = JsonConvert.SerializeObject(registrationInfo);
                HttpResponseMessage response;
                try
                {
                    progress.AddInfo("Registering DNS record(s)/challenge(s) in api.ravendb.net.");
                    progress.AddInfo("Please wait between 30 seconds and a few minutes.");
                    onProgress(progress);
                    response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/register",
                        new StringContent(serializeObject, Encoding.UTF8, "application/json"), token).ConfigureAwait(false);
                    progress.AddInfo("Waiting for DNS records to update...");
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

                if (challenge == null)
                {
                    var existingSubDomain =
                        registrationInfo.SubDomains.FirstOrDefault(x => x.SubDomain.StartsWith(setupInfo.LocalNodeTag + ".", StringComparison.OrdinalIgnoreCase));
                    if (existingSubDomain != null && new HashSet<string>(existingSubDomain.Ips).SetEquals(setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag].Addresses))
                    {
                        progress.AddInfo("DNS update started successfully, since current node (" + setupInfo.LocalNodeTag +
                                         ") DNS record didn't change, not waiting for full DNS propagation.");
                        return;
                    }
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
                            response = await ApiHttpClient.Instance.PostAsync("api/v1/dns-n-cert/registration-result?id=" + id,
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

        public static string IndentJsonString(string json)
        {
            using (var stringReader = new StringReader(json))
            using (var stringWriter = new StringWriter())
            {
                var jsonReader = new JsonTextReader(stringReader);
                var jsonWriter = new JsonTextWriter(stringWriter) {Formatting = Formatting.Indented};
                jsonWriter.WriteToken(jsonReader);
                return stringWriter.ToString();
            }
        }

        public static async Task<(byte[], CertificateDefinition, string)> GenerateCertificateTask(string name, RavenServer.CertificateHolder certificate,
            SetupInfo setupInfo)
        {
            if (certificate?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{name}' because the server certificate is not loaded.");
            //System.Security.Cryptography.X509Certificates
            // this creates a client certificate which is signed by the current server certificate
            
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(name, certificate, out var certBytes,
                setupInfo.ClientCertNotAfter ?? DateTime.UtcNow.Date.AddYears(5));

            var newCertDef = new CertificateDefinition
            {
                Name = name,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = selfSignedCertificate.Thumbprint,
                PublicKeyPinningHash = selfSignedCertificate.GetPublicKeyPinningHash(),
                NotAfter = selfSignedCertificate.NotAfter
            };

            return (certBytes, newCertDef, selfSignedCertificate.Thumbprint);
        }
    }
}
