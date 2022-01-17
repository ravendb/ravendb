using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Org.BouncyCastle.Crypto.Prng;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Raven.Server.Https;
using Raven.Server.ServerWide;
using Raven.Server.Utils.Cli;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Threading;
using Sparrow.Utils;
using StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;

namespace Raven.Server.Commercial.LetsEncrypt;

public class LetsEncryptApiHelper
{
    
    internal static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");

    internal class UniqueResponseResponder : IStartup
    {
        private readonly string _response;

        public UniqueResponseResponder(string response)
        {
            _response = response;
        }

        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
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
    
    public class CompleteAuthorizationAndGetCertificateParameters
    {
        public Action OnValidationSuccessful;
        public SetupInfo SetupInfo;
        public LetsEncryptClient Client;
        public (string Challange, LetsEncryptClient.CachedCertificateResult Cache) ChallengeResult;
        public ServerStore ServerStore;
        public CancellationToken Token;
    }

    public class CompleteClusterConfigurationParameters
    {
        public SetupProgressAndResult Progress;
        public Action<IOperationProgress> OnProgress;
        public SetupInfo SetupInfo;
        public Func<string, string, Task> OnBeforeAddingNodesToCluster;
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

    public static async Task<X509Certificate2> CompleteAuthorizationAndGetCertificate(CompleteAuthorizationAndGetCertificateParameters parameters)
    {
        if (parameters.ChallengeResult.Challange == null && parameters.ChallengeResult.Cache != null)
        {
            return BuildNewPfx(parameters.SetupInfo, parameters.ChallengeResult.Cache.Certificate, parameters.ChallengeResult.Cache.PrivateKey);
        }

        try
        {
            await parameters.Client.CompleteChallenges(parameters.Token);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to Complete Let's Encrypt challenge(s).", e);
        }

        if (parameters.OnValidationSuccessful == null)
        {
            Console.WriteLine("Let's encrypt validation successful, acquiring certificate now...");
        }
        else
        {
            parameters.OnValidationSuccessful();
        }

        (X509Certificate2 Cert, RSA PrivateKey) result;
        try
        {
            var existingPrivateKey = parameters.ServerStore?.Server.Certificate?.Certificate?.GetRSAPrivateKey();
            result = await parameters.Client.GetCertificate(existingPrivateKey, parameters.Token);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to acquire certificate from Let's Encrypt.", e);
        }

        try
        {
            return BuildNewPfx(parameters.SetupInfo, result.Cert, result.PrivateKey);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to build certificate from Let's Encrypt.", e);
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

        setupInfo.Certificate = Convert.ToBase64String(certBytes);

        return new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
    }

    public static async Task<byte[]> CompleteClusterConfigurationAndGetSettingsZip(CompleteClusterConfigurationParameters parameters)
    {
        try
        {
            await using (var ms = new MemoryStream())
            {
                using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
                {
                    parameters.Progress?.AddInfo("Loading and validating server certificate.");
                    if (parameters.Progress != null)
                    {
                        parameters.OnProgress(parameters.Progress);
                    }
                    else
                    {
                        Console.WriteLine("Loading and validating server certificate.");
                    }

                    byte[] serverCertBytes;
                    X509Certificate2 serverCert;
                    string domainFromCert;
                    string publicServerUrl;
                    LetsEncryptUtils.CertificateHolder serverCertificateHolder;

                    try
                    {
                        var base64 = parameters.SetupInfo.Certificate;
                        serverCertBytes = Convert.FromBase64String(base64);
                        serverCert = new X509Certificate2(serverCertBytes, parameters.SetupInfo.Password,
                            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

                        var localNodeTag = parameters.SetupInfo.LocalNodeTag;
                        publicServerUrl = LetsEncryptCertificateUtil.GetServerUrlFromCertificate(serverCert, parameters.SetupInfo, localNodeTag,
                            parameters.SetupInfo.NodeSetupInfos[localNodeTag].Port,
                            parameters.SetupInfo.NodeSetupInfos[localNodeTag].TcpPort, out _, out domainFromCert);

                        if (parameters.OnBeforeAddingNodesToCluster != null)
                            await parameters.OnBeforeAddingNodesToCluster(publicServerUrl, localNodeTag);

                        foreach (var node in parameters.SetupInfo.NodeSetupInfos)
                        {
                            if (node.Key == parameters.SetupInfo.LocalNodeTag)
                                continue;

                            if (parameters.Progress != null)
                                parameters.Progress.AddInfo($"Adding node '{node.Key}' to the cluster.");
                            else
                                Console.WriteLine($"Adding node '{node.Key}' to the cluster.");

                            parameters.OnProgress?.Invoke(parameters.Progress);


                            parameters.SetupInfo.NodeSetupInfos[node.Key].PublicServerUrl = LetsEncryptCertificateUtil.GetServerUrlFromCertificate(serverCert, parameters.SetupInfo, node.Key,
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

                    if (parameters.Progress != null)
                        parameters.Progress.AddInfo("Generating the client certificate.");
                    else
                        Console.WriteLine($"Generating the client certificate.");

                    parameters.OnProgress?.Invoke(parameters.Progress);

                    X509Certificate2 clientCert;

                    var name = (parameters.SetupMode == SetupMode.Secured)
                        ? domainFromCert.ToLower()
                        : parameters.SetupInfo.Domain.ToLower();

                    byte[] certBytes;
                    try
                    {
                        // requires server certificate to be loaded
                        var clientCertificateName = $"{name}.client.certificate";
                        var result = LetsEncryptCertificateUtil.GenerateCertificate(serverCertificateHolder, clientCertificateName, parameters.SetupInfo);
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
                        await parameters.RegisterClientCertInOs(parameters.OnProgress, parameters.Progress, clientCert);

                    if (parameters.Progress != null)
                        parameters.Progress?.AddInfo("Writing certificates to zip archive.");
                    else
                        Console.WriteLine("Writing certificates to zip archive.");

                    parameters.OnProgress?.Invoke(parameters.Progress);

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
                                await entryStream.WriteAsync(export, parameters.Token);
                            else
                                await entryStream.WriteAsync(export, CancellationToken.None);
                        }

                        await LetsEncryptCertificateUtil.WriteCertificateAsPemAsync($"admin.client.certificate.{name}", certBytes, null, archive);
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
                    string certPath;
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
                        currentNodeSettingsJson.Modifications ??= new DynamicJsonValue(currentNodeSettingsJson);

                        if (parameters.Progress != null)
                            parameters.Progress?.AddInfo($"Creating settings file 'settings.json' for node {node.Key}.");
                        else
                            Console.WriteLine($"Creating settings file 'settings.json' for node {node.Key}.");

                        parameters.OnProgress?.Invoke(parameters.Progress);

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
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException("Failed to write settings file 'settings.json' for the local sever.", e);
                            }
                        }

                        if (parameters.Progress != null)
                            parameters.Progress?.AddInfo($"Adding settings file for node '{node.Key}' to zip archive.");
                        else
                            Console.WriteLine($"Adding settings file for node '{node.Key}' to zip archive.");

                        parameters.OnProgress?.Invoke(parameters.Progress);

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

                    if (parameters.Progress != null)
                        parameters.Progress?.AddInfo("Adding readme file to zip archive.");
                    else
                        Console.WriteLine("Adding readme file to zip archive.");

                    parameters.OnProgress?.Invoke(parameters.Progress);

                    string readmeString = CreateReadmeText(parameters.SetupInfo.LocalNodeTag, publicServerUrl, parameters.SetupInfo.NodeSetupInfos.Count > 1,
                        parameters.SetupInfo.RegisterClientCert);

                    if (parameters.Progress != null)
                        parameters.Progress.Readme = readmeString;
                    else
                        Console.WriteLine(readmeString);

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

                    if (parameters.Progress != null)
                        parameters.Progress.AddInfo("Adding setup.json file to zip archive.");
                    else
                        Console.WriteLine("Adding setup.json file to zip archive.");

                    parameters.OnProgress?.Invoke(parameters.Progress);

                    try
                    {
                        var settings = new SetupSettings {Nodes = parameters.SetupInfo.NodeSetupInfos.Select(tag => new SetupSettings.Node {Tag = tag.Key}).ToArray()};

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
        catch (Exception e) when (e is UnauthorizedAccessException or SecurityException)
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
    
    public static string IpAddressToUrl(string address, int port)
    {
        var url = "https://" + address;
        if (port != 443)
            url += ":" + port;
        return url;
    }

    public  static string IpAddressToTcpUrl(string address, int port)
    {
        var url = "tcp://" + address;
        if (port != 0)
            url += ":" + port;
        return url;
    }
}
