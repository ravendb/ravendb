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
using JetBrains.Annotations;
using Newtonsoft.Json;
using Org.BouncyCastle.Asn1;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.OpenSsl;
using Raven.Server.Commercial;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Threading;
using Sparrow.Utils;
using OpenFlags = System.Security.Cryptography.X509Certificates.OpenFlags;
using StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;

namespace Raven.Server.Commercial
{
    public static class LetsEncryptUtils
    {
        private const string AcmeClientUrl = "https://acme-v02.api.letsencrypt.org/directory";

        public static async Task<byte[]> SetupLetsEncryptByRvn(SetupInfo setupInfo, CancellationToken token)
        {
            try
            {
                if (SetupManager.IsValidEmail(setupInfo.Email) == false)
                {
                    throw new ArgumentException("Invalid e-mail format" + setupInfo.Email);
                }

                var acmeClient = new LetsEncryptClient(AcmeClientUrl);
                await acmeClient.Init(setupInfo.Email, token);
                await CompleteClusterConfigurationAndGetSettingsZip(new CompleteClusterConfigurationParameters());
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Setting up RavenDB in Let's Encrypt security mode failed", e);
            }

            return null;
        }

        public class CompleteClusterConfigurationParameters
        {
            public SetupProgressAndResult Progress;
            public Action<IOperationProgress> onProgress;
            public SetupInfo SetupInfo;
            public Func<string, X509Certificate2, byte[], string, Task> OnBeforeAddingNodesToCluster;
            public Func<string, Task> AddNodeToCluster;
            public Func<Action<IOperationProgress>, SetupProgressAndResult, X509Certificate2, Task> RegisterClientCertInOs;
            public Func<StudioConfiguration.StudioEnvironment, Task> OnPutServerWideStudioConfigurationValues;
            public Func<string, Task> OnWriteSettingsJsonLocally;
            public Func<string, Task<string>> OnGetCertificatePath;
            public Func<string, Task> RegisterClientCert;
            public Func<CertificateDefinition, Task> PutCertificate;
            public SetupMode SetupMode;
            public string SettingsPath;
            public bool CertificateValidationKeyUsages;
            public LicenseType LicenseType;
            public CancellationToken Token = CancellationToken.None;
        }

        public class CertificateHolder
        {
            public string CertificateForClients;
            public X509Certificate2 Certificate;
            public AsymmetricKeyEntry PrivateKey;
        }


        public static (byte[] CertBytes, CertificateDefinition CertificateDefinition) GenerateCertificate(CertificateHolder certificateHolder, string certificateName,
            SetupInfo setupInfo)
        {
            if (certificateHolder == null)
                throw new InvalidOperationException(
                    $"Cannot generate the client certificate '{certificateName}' because the server certificate is not loaded.");

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificateName, certificateHolder, out var certBytes,
                setupInfo.ClientCertNotAfter ?? DateTime.UtcNow.Date.AddYears(5));

            var newCertDef = new CertificateDefinition
            {
                Name = certificateName,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = selfSignedCertificate.Thumbprint,
                PublicKeyPinningHash = selfSignedCertificate.GetPublicKeyPinningHash(),
                NotAfter = selfSignedCertificate.NotAfter
            };

            return (certBytes, newCertDef);
        }

        public static async Task PutValuesAfterGenerateCertificateTask(ServerStore serverStore, string thumbprint, CertificateDefinition certificateDefinition)
        {
            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(thumbprint, certificateDefinition, RaftIdGenerator.DontCareId));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);
        }

        public static async Task OnPutServerWideStudioConfigurationValues(ServerStore serverStore, StudioConfiguration.StudioEnvironment studioEnvironment)
        {
            var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(
                new ServerWideStudioConfiguration {Disabled = false, Environment = studioEnvironment}, RaftIdGenerator.DontCareId));

            await serverStore.Cluster.WaitForIndexNotification(res.Index);
        }

        public static async Task<byte[]> CompleteClusterConfigurationAndGetSettingsZip(CompleteClusterConfigurationParameters parameters)
        {
            try
            {
                //var settingsPath = serverStore.Configuration.ConfigPath;
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
                    {
                        parameters.Progress?.AddInfo("Loading and validating server certificate.");
                        parameters.onProgress(parameters.Progress);
                        byte[] serverCertBytes;
                        X509Certificate2 serverCert;
                        string domainFromCert;
                        string publicServerUrl;
                        CertificateHolder serverCertificateHolder;
                        // (byte[] CertBytes, CertificateDefinition CertificateDefinition) result;

                        try
                        {
                            var base64 = parameters.SetupInfo.Certificate;
                            serverCertBytes = Convert.FromBase64String(base64);
                            serverCert = new X509Certificate2(serverCertBytes, parameters.SetupInfo.Password,
                                X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

                            var localNodeTag = parameters.SetupInfo.LocalNodeTag;
                            publicServerUrl = GetServerUrlFromCertificate(serverCert, parameters.SetupInfo, localNodeTag,
                                parameters.SetupInfo.NodeSetupInfos[localNodeTag].Port,
                                parameters.SetupInfo.NodeSetupInfos[localNodeTag].TcpPort, out _, out domainFromCert);

                            if (parameters.OnBeforeAddingNodesToCluster != null)
                                await parameters.OnBeforeAddingNodesToCluster(publicServerUrl, serverCert, serverCertBytes, localNodeTag);

                            foreach (var node in parameters.SetupInfo.NodeSetupInfos)
                            {
                                if (node.Key == parameters.SetupInfo.LocalNodeTag)
                                    continue;

                                parameters.Progress?.AddInfo($"Adding node '{node.Key}' to the cluster.");
                                parameters.onProgress(parameters.Progress);

                                parameters.SetupInfo.NodeSetupInfos[node.Key].PublicServerUrl = GetServerUrlFromCertificate(serverCert, parameters.SetupInfo, node.Key,
                                    node.Value.Port,
                                    node.Value.TcpPort, out _, out _);

                                if (parameters.AddNodeToCluster != null)
                                    await parameters.AddNodeToCluster(node.Key);
                            }

                            serverCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup", serverCert, serverCertBytes,
                                parameters.SetupInfo.Password, parameters.LicenseType, parameters.CertificateValidationKeyUsages);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Could not load the certificate in the local server.", e);
                        }

                        parameters.Progress?.AddInfo("Generating the client certificate.");
                        parameters.onProgress(parameters.Progress);
                        X509Certificate2 clientCert;

                        var name = (parameters.SetupMode == SetupMode.Secured)
                            ? domainFromCert.ToLower()
                            : parameters.SetupInfo.Domain.ToLower();

                        byte[] certBytes;
                        try
                        {
                            // requires server certificate to be loaded
                            var clientCertificateName = $"{name}.client.certificate";
                            var result = GenerateCertificate(serverCertificateHolder, clientCertificateName, parameters.SetupInfo);
                            certBytes = result.CertBytes;

                            if (parameters.PutCertificate != null)
                                await parameters.PutCertificate(result.CertificateDefinition);

                            clientCert = new X509Certificate2(certBytes, (string)null,
                                X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException($"Could not generate a client certificate for '{name}'.", e);
                        }


                        if (parameters.RegisterClientCert != null)
                            await parameters.RegisterClientCertInOs(parameters.onProgress, parameters.Progress, clientCert);

                        parameters.Progress?.AddInfo("Writing certificates to zip archive.");
                        parameters.onProgress(parameters.Progress);

                        try
                        {
                            var entry = archive.CreateEntry($"admin.client.certificate.{name}.pfx");

                            // Structure of external attributes field: https://unix.stackexchange.com/questions/14705/the-zip-formats-external-file-attribute/14727#14727
                            // The permissions go into the most significant 16 bits of an int
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                            await using (var entryStream = entry.Open())
                            {
                                var export = clientCert.Export(X509ContentType.Pfx);
                                if (parameters.Token != CancellationToken.None)
                                    await entryStream.WriteAsync(export, 0, export.Length, parameters.Token);
                                else
                                    await entryStream.WriteAsync(export, 0, export.Length, CancellationToken.None);
                            }

                            await WriteCertificateAsPemAsync($"admin.client.certificate.{name}", certBytes, null, archive);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write the certificates to a zip archive.", e);
                        }

                        BlittableJsonReaderObject settingsJson;
                        await using (var fs = SafeFileStream.Create(parameters.SettingsPath, FileMode.Open, FileAccess.Read))
                        {
                            settingsJson = await context.ReadForMemoryAsync(fs, "settings-json");
                        }

                        settingsJson.Modifications = new DynamicJsonValue(settingsJson);

                        if (parameters.SetupMode == SetupMode.LetsEncrypt)
                        {
                            settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = parameters.SetupInfo.Email;

                            try
                            {
                                var licenseString = JsonConvert.SerializeObject(parameters.SetupInfo.License, Formatting.Indented);

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

                        settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.SetupMode)] = parameters.SetupMode.ToString();

                        if (parameters.SetupInfo.EnableExperimentalFeatures)
                        {
                            settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = FeaturesAvailability.Experimental;
                        }

                        if (parameters.SetupInfo.Environment != StudioConfiguration.StudioEnvironment.None)
                        {
                            if (parameters.OnPutServerWideStudioConfigurationValues != null)
                                await parameters.OnPutServerWideStudioConfigurationValues(parameters.SetupInfo.Environment);
                        }

                        var certificateFileName = $"cluster.server.certificate.{name}.pfx";
                        string certPath = "";
                        if (parameters.OnGetCertificatePath != null)
                            certPath = await parameters.OnGetCertificatePath(certificateFileName);
                        else
                            certPath = Path.Combine(AppContext.BaseDirectory, certificateFileName);

                        if (parameters.SetupInfo.ModifyLocalServer)
                        {
                            await using (var certFile = SafeFileStream.Create(certPath, FileMode.Create))
                            {
                                if (parameters.Token != CancellationToken.None)
                                {
                                    await certFile.WriteAsync(serverCertBytes, 0, serverCertBytes.Length, parameters.Token);
                                    await certFile.FlushAsync(parameters.Token);
                                }
                                else
                                {
                                    await certFile.WriteAsync(serverCertBytes, 0, serverCertBytes.Length, CancellationToken.None);
                                    await certFile.FlushAsync(CancellationToken.None);
                                }
                            } // we'll be flushing the directory when we'll write the settings.json
                        }

                        settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certPath;
                        if (string.IsNullOrEmpty(parameters.SetupInfo.Password) == false)
                            settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = parameters.SetupInfo.Password;

                        foreach (var node in parameters.SetupInfo.NodeSetupInfos)
                        {
                            var currentNodeSettingsJson = settingsJson.Clone(context);
                            currentNodeSettingsJson.Modifications = currentNodeSettingsJson.Modifications ?? new DynamicJsonValue(currentNodeSettingsJson);

                            parameters.Progress?.AddInfo($"Creating settings file 'settings.json' for node {node.Key}.");
                            parameters.onProgress(parameters.Progress);

                            if (node.Value.Addresses.Count != 0)
                            {
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] =
                                    string.Join(";", node.Value.Addresses.Select(ip => IpAddressToUrl(ip, node.Value.Port)));
                                currentNodeSettingsJson.Modifications[RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] =
                                    string.Join(";", node.Value.Addresses.Select(ip => IpAddressToTcpUrl(ip, node.Value.TcpPort)));
                            }

                            var httpUrl = GetServerUrlFromCertificate(serverCert, parameters.SetupInfo, node.Key, node.Value.Port,
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
                            if (node.Key == parameters.SetupInfo.LocalNodeTag && parameters.SetupInfo.ModifyLocalServer)
                            {
                                try
                                {
                                    if (parameters.OnWriteSettingsJsonLocally != null)
                                        await parameters.OnWriteSettingsJsonLocally(indentedJson);
                                    else
                                        WriteSettingsJsonLocally(parameters.SettingsPath, indentedJson);
                                    // WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson);
                                }
                                catch (Exception e)
                                {
                                    throw new InvalidOperationException("Failed to write settings file 'settings.json' for the local sever.", e);
                                }
                            }

                            parameters.Progress?.AddInfo($"Adding settings file for node '{node.Key}' to zip archive.");
                            parameters.onProgress(parameters.Progress);

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

                        parameters.Progress?.AddInfo("Adding readme file to zip archive.");
                        parameters.onProgress(parameters.Progress);

                        string readmeString = CreateReadmeText(parameters.SetupInfo.LocalNodeTag, publicServerUrl, parameters.SetupInfo.NodeSetupInfos.Count > 1, parameters.SetupInfo.RegisterClientCert);

                        parameters.Progress.Readme = readmeString;
                        try
                        {
                            var entry = archive.CreateEntry("readme.txt");
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                            await using (var entryStream = entry.Open())
                            await using (var writer = new StreamWriter(entryStream))
                            {
                                await writer.WriteAsync(readmeString);
                                await writer.FlushAsync();
                                if (parameters.Token != CancellationToken.None)
                                    await entryStream.FlushAsync(parameters.Token);
                                else
                                    await entryStream.FlushAsync(CancellationToken.None);
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Failed to write readme.txt to zip archive.", e);
                        }

                        parameters.Progress?.AddInfo("Adding setup.json file to zip archive.");
                        parameters.onProgress(parameters.Progress);

                        try
                        {
                            var settings = new SetupSettings
                            {
                                Nodes = parameters.SetupInfo.NodeSetupInfos.Select(tag => new SetupSettings.Node {Tag = tag.Key}).ToArray()
                            };

                            var modifiedJsonObj = context.ReadObject(settings.ToJson(), "setup-json");

                            var indentedJson = IndentJsonString(modifiedJsonObj.ToString());

                            var entry = archive.CreateEntry("setup.json");
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                            await using (var entryStream = entry.Open())
                            await using (var writer = new StreamWriter(entryStream))
                            {
                                await writer.WriteAsync(indentedJson);
                                await writer.FlushAsync();
                                await entryStream.FlushAsync(parameters.Token);
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

        public static string GetServerUrlFromCertificate(X509Certificate2 cert, SetupInfo setupInfo, string nodeTag, int port, int tcpPort, out string publicTcpUrl,
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

        private static string IpAddressToUrl(string address, int port)
        {
            var url = "https://" + address;
            if (port != 443)
                url += ":" + port;
            return url;
        }

        private static string IpAddressToTcpUrl(string address, int port)
        {
            var url = "tcp://" + address;
            if (port != 0)
                url += ":" + port;
            return url;
        }

        public static void WriteSettingsJsonLocally(string settingsPath, string json)
        {
            var tmpPath = string.Empty;
            try
            {
                tmpPath = settingsPath + ".tmp";
                using (var file = SafeFileStream.Create(tmpPath, FileMode.Create))
                using (var writer = new StreamWriter(file))
                {
                    writer.Write(json);
                    writer.Flush();
                    file.Flush(true);
                }
            }
            catch (Exception e) when (e is UnauthorizedAccessException || e is SecurityException)
            {
                throw new UnsuccessfulFileAccessException(e, tmpPath, FileAccess.Write);
            }

            try
            {
                File.Replace(tmpPath, settingsPath, settingsPath + ".bak");
                if (PlatformDetails.RunningOnPosix)
                    Syscall.FsyncDirectoryFor(settingsPath);
            }
            catch (UnauthorizedAccessException e)
            {
                throw new UnsuccessfulFileAccessException(e, settingsPath, FileAccess.Write);
            }
        }

        public static string CreateReadmeText(string nodeTag, string publicServerUrl, bool isCluster, bool registerClientCert)
        {
            var str =
                string.Format(WelcomeMessage.AsciiHeader, Environment.NewLine) + Environment.NewLine + Environment.NewLine +
                "Your RavenDB cluster settings, certificate and configuration are contained in this zip file."
                + Environment.NewLine;

            str += Environment.NewLine +
                   $"The new server is available at: {publicServerUrl}"
                   + Environment.NewLine;

            str += $"The current node ('{nodeTag}') has already been configured and requires no further action on your part." +
                   Environment.NewLine;

            str += Environment.NewLine;
            if (registerClientCert && PlatformDetails.RunningOnPosix == false)
            {
                str +=
                    $"An administrator client certificate has been installed on this machine ({Environment.MachineName})." +
                    Environment.NewLine +
                    $"You can now restart the server and access the studio at {publicServerUrl}." +
                    Environment.NewLine +
                    "Chrome will let you select this certificate automatically. " +
                    Environment.NewLine +
                    "If it doesn't, you will get an authentication error. Please restart all instances of Chrome to make sure nothing is cached." +
                    Environment.NewLine;
            }
            else
            {
                str +=
                    "An administrator client certificate has been generated and is located in the zip file." +
                    Environment.NewLine +
                    $"However, the certificate was not installed on this machine ({Environment.MachineName}), this can be done manually." +
                    Environment.NewLine;
            }

            str +=
                "If you are using Firefox (or Chrome under Linux), the certificate must be imported manually to the browser." +
                Environment.NewLine +
                "You can do that via: Tools > Options > Advanced > 'Certificates: View Certificates'." +
                Environment.NewLine;

            if (PlatformDetails.RunningOnPosix)
                str +=
                    "In Linux, importing the client certificate to the browser might fail for 'Unknown Reasons'." +
                    Environment.NewLine +
                    "If you encounter this bug, use the RavenCli command 'generateClientCert' to create a new certificate with a password." +
                    Environment.NewLine +
                    "For more information on this workaround, read the security documentation in 'ravendb.net'." +
                    Environment.NewLine;

            str +=
                Environment.NewLine +
                "It is recommended to generate additional certificates with reduced access rights for applications and users." +
                Environment.NewLine +
                "This can be done using the RavenDB Studio, in the 'Manage Server' > 'Certificates' page." +
                Environment.NewLine;

            if (isCluster)
            {
                str +=
                    Environment.NewLine +
                    "You are setting up a cluster. The cluster topology and node addresses have already been configured." +
                    Environment.NewLine +
                    "The next step is to download a new RavenDB server for each of the other nodes." +
                    Environment.NewLine +
                    Environment.NewLine +
                    "When you enter the setup wizard on a new node, please choose 'Continue Existing Cluster Setup'." +
                    Environment.NewLine +
                    "Do not try to start a new setup process again in this new node, it is not supported." +
                    Environment.NewLine +
                    "You will be asked to upload the zip file which was just downloaded." +
                    Environment.NewLine +
                    "The new server node will join the already existing cluster." +
                    Environment.NewLine +
                    Environment.NewLine +
                    "When the wizard is done and the new node was restarted, the cluster will automatically detect it. " +
                    Environment.NewLine +
                    "There is no need to manually add it again from the studio. Simply access the 'Cluster' view and " +
                    Environment.NewLine +
                    "observe the topology being updated." +
                    Environment.NewLine;
            }

            return str;
        }
    }
}
