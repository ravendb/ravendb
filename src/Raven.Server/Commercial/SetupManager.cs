using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.Commercial.SetupWizard;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Sparrow.Utils;

namespace Raven.Server.Commercial
{
    public static class SetupManager
    {
        internal static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        
        private static string BuildHostName(string nodeTag, string userDomain, string rootDomain)
        {
            return $"{nodeTag}.{userDomain}.{rootDomain}".ToLower();
        }

        public static async Task<string> LetsEncryptAgreement(string email, ServerStore serverStore)
        {
            if (EmailValidator.IsValid(email) == false)
                throw new ArgumentException("Invalid e-mail format" + email);

            var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
            await acmeClient.Init(email);
            return acmeClient.GetTermsOfServiceUri();
        }

        public static async Task<IOperationResult> SetupUnsecuredTask(Action<IOperationProgress> onProgress, UnsecuredSetupInfo unsecuredSetupInfo,
            ServerStore serverStore, CancellationToken token)
        {
            var zipOnly = unsecuredSetupInfo.ZipOnly;
            var progress = new SetupProgressAndResult(tuple =>
            {
                if (Logger is {IsInfoEnabled: true})
                    Logger.Info(tuple.Message, tuple.Exception);
            });

            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo("Setting up RavenDB in 'Unsecured Mode'.");
                progress.AddInfo("Starting validation.");
                onProgress(progress);

                try
                {
                    unsecuredSetupInfo.ValidateInfo(new CreateSetupPackageParameters {UnsecuredSetupInfo = unsecuredSetupInfo});
                }
                catch (Exception e)
                {
                    throw new AggregateException(e);
                }

                await ValidateUnsecuredServerCanRunWithSuppliedSettings(unsecuredSetupInfo, serverStore, token);

                progress.Processed++;
                progress.AddInfo("Validation is successful.");
                progress.AddInfo("Creating new RavenDB configuration settings.");
                onProgress(progress);

                try
                {

                    var completeClusterConfigurationResult = await CompleteClusterConfigurationUnsecuredSetup(onProgress,
                        progress,
                        SetupMode.Unsecured,
                        unsecuredSetupInfo,
                        serverStore,
                        token);

                    progress.SettingsZipFile = await SettingsZipFileHelper.GetSetupZipFileUnsecuredSetup(new GetSetupZipFileParameters
                    {
                        CompleteClusterConfigurationResult = completeClusterConfigurationResult,
                        Progress = progress,
                        ZipOnly = zipOnly,
                        OnProgress = onProgress,
                        OnSettingsPath = () => serverStore.Configuration.ConfigPath,
                        UnsecuredSetupInfo = unsecuredSetupInfo,
                        SetupMode = SetupMode.Unsecured,
                        Token = token,
                        OnWriteSettingsJsonLocally = indentedJson => SettingsZipFileHelper.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson),
                        OnPutServerWideStudioConfigurationValues = async studioEnvironment =>
                        {
                            var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(
                                new ServerWideStudioConfiguration
                                {
                                    Disabled = false,
                                    Environment = studioEnvironment
                                },
                                RaftIdGenerator.DontCareId));
                            await serverStore.Cluster.WaitForIndexNotification(res.Index);
                        }
                    });
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not create configuration settings.", e);
                }

                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in 'Unsecured Mode' finished successfully.");
                onProgress(progress);
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in 'Unsecured Mode' failed.", e);
            }

            return progress;
        }

        public static async Task<IOperationResult> SetupSecuredTask(Action<IOperationProgress> onProgress, SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
        {
            var progress = new SetupProgressAndResult(tuple =>
            {
                if (Logger is {IsInfoEnabled: true})
                    Logger.Info(tuple.Message, tuple.Exception);
            });

            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo("Setting up RavenDB in 'Secured Mode'.");
                progress.AddInfo("Starting validation.");
                onProgress(progress);

                await LetsEncryptValidationHelper.ValidateSetupInfo(SetupMode.Secured, setupInfo, serverStore);

                try
                {
                    await ValidateServerCanRunWithSuppliedSettings(setupInfo, serverStore, SetupMode.Secured, token);
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
                    var completeClusterConfigurationResult = await CompleteClusterConfigurationAndGetSettingsZipSecuredSetup(onProgress, progress, SetupMode.Secured, setupInfo, serverStore, token);

                    progress.SettingsZipFile = await SettingsZipFileHelper.GetSetupZipFileSecuredSetup(new GetSetupZipFileParameters
                    {
                        CompleteClusterConfigurationResult = completeClusterConfigurationResult,
                        Progress = progress,
                        OnProgress = onProgress,
                        OnSettingsPath = () => serverStore.Configuration.ConfigPath,
                        SetupInfo = setupInfo,
                        SetupMode = SetupMode.Secured,
                        Token = token,
                        OnWriteSettingsJsonLocally = indentedJson => SettingsZipFileHelper.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson),
                        OnGetCertificatePath = certificateFileName =>
                        {
                            return serverStore.Configuration.GetSetting(RavenConfiguration.GetKey(x => x.Core.SetupResultingServerCertificatePath)) ?? Path.Combine(AppContext.BaseDirectory, certificateFileName);
                        },
                        OnPutServerWideStudioConfigurationValues = async studioEnvironment =>
                        {
                            var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(new ServerWideStudioConfiguration
                            {
                                Disabled = false,
                                Environment = studioEnvironment
                            }, RaftIdGenerator.DontCareId));
                            await serverStore.Cluster.WaitForIndexNotification(res.Index);
                }
                    });
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

        public static async Task<X509Certificate2> RefreshLetsEncryptTask(SetupInfo setupInfo, ServerStore serverStore, CancellationToken token)
        {
            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
            await acmeClient.Init(setupInfo.Email, token);

            // here we explicitly want to refresh the cert, so we don't want it cached
            var cacheKeys = setupInfo.NodeSetupInfos.Select(node => BuildHostName(node.Key, setupInfo.Domain, setupInfo.RootDomain)).ToList();
            acmeClient.ResetCachedCertificate(cacheKeys);

            var challengeResult = await LetsEncryptSetupUtils.InitialLetsEncryptChallenge(setupInfo, acmeClient, token);

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");

            try
            {
                await RavenDnsRecordHelper.UpdateDnsRecordsForCertificateRefreshTask(challengeResult.Challenge, setupInfo, Logger, token);

                // Cache the current DNS topology so we can check it again
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
            }

            if (Logger.IsOperationsEnabled)
                Logger.Operations($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");

            var cert = await CertificateUtils.CompleteAuthorizationAndGetCertificate(
                new CompleteAuthorizationAndGetCertificateParameters
                {
                    OnValidationSuccessful = () =>
                    {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Let's encrypt validation successful, acquiring certificate now...");
                },
                    SetupInfo = setupInfo,
                    Client = acmeClient,
                    ChallengeResult = challengeResult,
                    ExistingPrivateKey = serverStore.Server.Certificate?.Certificate?.GetRSAPrivateKey(),
                    Token = token
                });

            if (Logger.IsOperationsEnabled)
                Logger.Operations("Successfully acquired certificate from Let's Encrypt.");

            return cert;
        }

        public static async Task<IOperationResult> ContinueUnsecuredClusterSetupTask(Action<IOperationProgress> onProgress, ContinueSetupInfo continueSetupInfo, ServerStore serverStore, CancellationToken token)
        {
            var progress = new SetupProgressAndResult(tuple =>
            {
                if (Logger is {IsInfoEnabled: true})
                    Logger.Info(tuple.Message, tuple.Exception);
            })
            {
                Processed = 0,
                Total = 4
            };
            
            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo($"Continuing cluster setup on node {continueSetupInfo.NodeTag}.");
                onProgress(progress);

                byte[] zipBytes;

                try
                {
                    zipBytes = Convert.FromBase64String(continueSetupInfo.Zip);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(continueSetupInfo.Zip)} property, expected a Base64 value", e);
                }

                progress.Processed++;
                progress.AddInfo("Extracting setup settings from zip file.");
                onProgress(progress);

                using (serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    BlittableJsonReaderObject settingsJsonObject;
                    Dictionary<string, string> otherNodesUrls;
                    string firstNodeTag;
                    License license;
                    try
                    {
                        settingsJsonObject = ExtractCertificatesAndSettingsJsonFromZip(zipBytes,
                            currentNodeTag: continueSetupInfo.NodeTag,
                            context: context,
                            certBytes: out _,
                            serverCert: out _,
                            clientCert: out _,
                            firstNodeTag: out firstNodeTag,
                            otherNodesUrls: out otherNodesUrls,
                            license: out license,
                            isSecured: false);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Starting validation.");
                    onProgress(progress);

                    try
                    {
                        await LetsEncryptValidationHelper.ValidateServerCanRunOnThisNode(settingsJsonObject, null, serverStore, continueSetupInfo.NodeTag, token);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Validation failed.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Validation is successful.");
                    progress.AddInfo("Writing configuration settings and certificate.");
                    onProgress(progress);

                    try
                    {
                        await CompleteUnsecuredConfigurationForNewNode(onProgress, progress, continueSetupInfo, settingsJsonObject, serverStore, firstNodeTag, otherNodesUrls, license, context);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Could not complete configuration for new node.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Configuration settings created.");
                    progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                    onProgress(progress);

                    settingsJsonObject.Dispose();
                }
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, $"Cluster setup on node {continueSetupInfo.NodeTag} has failed", e);
            }

            return progress;
        }

        public static async Task<IOperationResult> ContinueClusterSetupTask(Action<IOperationProgress> onProgress, ContinueSetupInfo continueSetupInfo, ServerStore serverStore, CancellationToken token)
        {
            var progress = new SetupProgressAndResult(tuple =>
            {
                if (Logger is {IsInfoEnabled: true})
                    Logger.Info(tuple.Message, tuple.Exception);
            })
            {
                Processed = 0,
                Total = 4
            };

            try
            {
                AssertNoClusterDefined(serverStore);

                progress.AddInfo($"Continuing cluster setup on node {continueSetupInfo.NodeTag}.");
                onProgress(progress);

                byte[] zipBytes;

                try
                {
                    zipBytes = Convert.FromBase64String(continueSetupInfo.Zip);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(continueSetupInfo.Zip)} property, expected a Base64 value", e);
                }

                progress.Processed++;
                progress.AddInfo("Extracting setup settings and certificates from zip file.");
                onProgress(progress);

                using (serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    byte[] serverCertBytes;
                    BlittableJsonReaderObject settingsJsonObject;
                    Dictionary<string, string> otherNodesUrls;
                    string firstNodeTag;
                    License license;
                    X509Certificate2 clientCert;
                    X509Certificate2 serverCert;
                    try
                    {
                        settingsJsonObject = ExtractCertificatesAndSettingsJsonFromZip(
                            zipBytes: zipBytes, 
                            currentNodeTag: continueSetupInfo.NodeTag,
                            context: context,
                            certBytes: out serverCertBytes, 
                            serverCert: out serverCert,
                            clientCert: out clientCert,
                            firstNodeTag: out firstNodeTag,
                            otherNodesUrls: out otherNodesUrls,
                            license: out license);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Starting validation.");
                    onProgress(progress);     

                    try
                    {
                        await LetsEncryptValidationHelper.ValidateServerCanRunOnThisNode(settingsJsonObject, serverCert, serverStore, continueSetupInfo.NodeTag, token);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Validation failed.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Validation is successful.");
                    progress.AddInfo("Writing configuration settings and certificate.");
                    onProgress(progress);

                    try
                    {
                        await CompleteSecuredConfigurationForNewNode(onProgress, progress, continueSetupInfo, settingsJsonObject, serverCertBytes, serverCert,
                            clientCert, serverStore, firstNodeTag, otherNodesUrls, license, context);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Could not complete configuration for new node.", e);
                    }

                    progress.Processed++;
                    progress.AddInfo("Configuration settings created.");
                    progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                    onProgress(progress);

                    settingsJsonObject.Dispose();
                }
            }
            catch (Exception e)
            {
                LogErrorAndThrow(onProgress, progress, $"Cluster setup on node {continueSetupInfo.NodeTag} has failed", e);
            }

            return progress;
        }

        public static async Task<LicenseStatus> GetUpdatedLicenseStatus(ServerStore serverStore, License currentLicense, Reference<License> updatedLicense = null)
        {
            var license = await serverStore.LicenseManager.GetUpdatedLicense(currentLicense).ConfigureAwait(false) ?? currentLicense;

            var licenseStatus = LicenseManager.GetLicenseStatus(license);
            if (licenseStatus.Expired)
                throw new LicenseExpiredException($"The provided license for {license.Name} has expired ({licenseStatus.Expiration})");

            if (updatedLicense != null)
                updatedLicense.Value = license;

            return licenseStatus;
        }

        private static void AssertNoClusterDefined(ServerStore serverStore)
        {
            var allNodes = serverStore.GetClusterTopology().AllNodes;
            if (allNodes.Count > 1)
            {
                throw new InvalidOperationException("This node is part of an already configured cluster and cannot be setup automatically any longer." +
                                                    Environment.NewLine +
                                                    "Either setup manually by editing the 'settings.json' file or delete the existing cluster, restart the server and try running setup again." +
                                                    Environment.NewLine +
                                                    "Existing cluster nodes " + JsonConvert.SerializeObject(allNodes, Formatting.Indented)
                                                    );
            }
        }

        [DoesNotReturn]
        private static void LogErrorAndThrow(Action<IOperationProgress> onProgress, SetupProgressAndResult progress, string msg, Exception e)
        {
            progress.AddError(msg, e);
            onProgress.Invoke(progress);
            throw new InvalidOperationException(msg, e);
        }
        internal static Task ValidateUnsecuredServerCanRunWithSuppliedSettings(UnsecuredSetupInfo unsecuredSetupInfo, ServerStore serverStore, CancellationToken token)
        {
            var localServerIp = unsecuredSetupInfo.NodeSetupInfos.Values.First();
            var nodes = unsecuredSetupInfo.NodeSetupInfos.Values.Where(x => x != localServerIp);
            try
            {
                // Evaluate current system tcp connections. This is the same information provided
                // by the netstat command line application, just in .Net strongly-typed object
                // form.  We will look through the list, and if our port we would like to use
                // in our TcpClient is occupied, we will set throw exception accordingly.
                IPGlobalProperties ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
                IPEndPoint[] tcpConnInfoArray = ipGlobalProperties.GetActiveTcpListeners();

                foreach (var node in nodes)
                {
                    foreach (IPEndPoint endpoint in tcpConnInfoArray)
                    {
                        if (endpoint.Port == node.Port)
                        {
                            throw new PortInUseException(endpoint.Port, endpoint.Address.ToString() ," Port is already in use");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to validate running the server with the supplied settings: ",ex);
            }

            return Task.CompletedTask;
        }

        internal static async Task ValidateServerCanRunWithSuppliedSettings(SetupInfo setupInfo, ServerStore serverStore, SetupMode setupMode, CancellationToken token)
        {
            var localNode = setupInfo.NodeSetupInfos[setupInfo.LocalNodeTag];
            var localIps = new List<IPEndPoint>();

            foreach (var hostnameOrIp in localNode.Addresses)
            {
                if (hostnameOrIp.Equals(Constants.Network.AnyIp))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(hostnameOrIp), localNode.Port));
                    continue;
                }

                foreach (var ip in await Dns.GetHostAddressesAsync(hostnameOrIp, token))
                {
                    localIps.Add(new IPEndPoint(IPAddress.Parse(ip.ToString()), localNode.Port));
                }
            }

            var serverCert = setupInfo.GetX509Certificate();

            var localServerUrl =
                CertificateUtils.GetServerUrlFromCertificate(serverCert, setupInfo, setupInfo.LocalNodeTag, localNode.Port, localNode.TcpPort, out _, out _);

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
                await LetsEncryptSimulationHelper.SimulateRunningServer(serverStore, serverCert, localServerUrl, setupInfo.LocalNodeTag, localIps.ToArray(),
                    localNode.Port,
                    serverStore.Configuration.ConfigPath, setupMode, token);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to simulate running the server with the supplied settings using: " + localServerUrl, e);
            }
        }

    public static async Task<IOperationResult> SetupLetsEncryptTask(Action<IOperationProgress> onProgress, SetupInfo setupInfo, ServerStore serverStore,
        CancellationToken token)
        {
        var progress = new SetupProgressAndResult(tuple =>
            {
            if (Logger is {IsInfoEnabled: true})
                Logger.Info(tuple.Message, tuple.Exception);
        })
                {
            Processed = 0,
            Total = 4
        };

        try
                {
            var updatedLicense = new Reference<License>();
            await GetUpdatedLicenseStatus(serverStore, setupInfo.License, updatedLicense).ConfigureAwait(false);
            setupInfo.License = updatedLicense.Value;

            AssertNoClusterDefined(serverStore);
            progress.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
            onProgress(progress);
            try
            {
                await LetsEncryptValidationHelper.ValidateSetupInfo(SetupMode.LetsEncrypt, setupInfo, serverStore);
            }
            catch (Exception e)
                {
                throw new InvalidOperationException("Validation of supplied settings failed.", e);
            }

            progress.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");
            onProgress(progress);

            var acmeClient = new LetsEncryptClient(serverStore.Configuration.Core.AcmeUrl);
            await acmeClient.Init(setupInfo.Email, token);

            var challengeResult = await LetsEncryptSetupUtils.InitialLetsEncryptChallenge(setupInfo, acmeClient, token);

            progress.Processed++;
            progress.AddInfo(challengeResult.Challenge != null ? "Successfully received challenge(s) information from Let's Encrypt." : "Using cached Let's Encrypt certificate.");

            progress.AddInfo($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");

            onProgress(progress);

            try
        {
                await RavenDnsRecordHelper.UpdateDnsRecordsTask(new UpdateDnsRecordParameters
            {
                    OnProgress = onProgress,
                    Progress = progress,
                    Challenge = challengeResult.Challenge,
                    SetupInfo = setupInfo,
                    Token = token
                });
                }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
            }

            progress.Processed++;
            progress.AddInfo($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");
            progress.AddInfo("Completing Let's Encrypt challenge(s)...");
            onProgress(progress);

            await CertificateUtils.CompleteAuthorizationAndGetCertificate(new CompleteAuthorizationAndGetCertificateParameters
            {
                OnValidationSuccessful = () =>
                {
                    progress.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                    progress.AddInfo("Acquiring certificate.");
                    onProgress(progress);
                },
                SetupInfo = setupInfo,
                Client = acmeClient,
                ChallengeResult = challengeResult,
                ExistingPrivateKey = serverStore.Server.Certificate?.Certificate?.GetRSAPrivateKey(),
                Token = token
            });


            progress.Processed++;
            progress.AddInfo("Successfully acquired certificate from Let's Encrypt.");
            progress.AddInfo("Starting validation.");
            onProgress(progress);

            try
            {
                await ValidateServerCanRunWithSuppliedSettings(setupInfo, serverStore, SetupMode.LetsEncrypt, token);
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
                var completeClusterConfigurationResult = await CompleteClusterConfigurationAndGetSettingsZipSecuredSetup(onProgress, progress, SetupMode.LetsEncrypt, setupInfo, serverStore, token);

                progress.SettingsZipFile = await SettingsZipFileHelper.GetSetupZipFileSecuredSetup(new GetSetupZipFileParameters
                {
                    CompleteClusterConfigurationResult = completeClusterConfigurationResult,
                    Progress = progress,
                    OnProgress = onProgress,
                    OnSettingsPath = () => serverStore.Configuration.ConfigPath,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.LetsEncrypt,
                    ZipOnly = true,
                    OnWriteSettingsJsonLocally = indentedJson => SettingsZipFileHelper.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson),
                    OnGetCertificatePath = certificateFileName =>
            {
                        return serverStore.Configuration.GetSetting(RavenConfiguration.GetKey(x => x.Core.SetupResultingServerCertificatePath)) ??
                               Path.Combine(AppContext.BaseDirectory, certificateFileName);
                    },
                    OnPutServerWideStudioConfigurationValues = async studioEnvironment =>
                {
                        var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(new ServerWideStudioConfiguration
            {
                            Disabled = false,
                            Environment = studioEnvironment
                        }, RaftIdGenerator.DontCareId));

                        await serverStore.Cluster.WaitForIndexNotification(res.Index);
                    },
                    Token = token
                });
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
        catch (Exception e)
            {
            LogErrorAndThrow(onProgress, progress, "Setting up RavenDB in Let's Encrypt security mode failed.", e);
            }

        return progress;
        }

        private static async Task CompleteSecuredConfigurationForNewNode(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            ContinueSetupInfo continueSetupInfo,
            BlittableJsonReaderObject settingsJsonObject,
            byte[] serverCertBytes,
            X509Certificate2 serverCert,
            X509Certificate2 clientCert,
            ServerStore serverStore,
            string firstNodeTag,
            Dictionary<string, string> otherNodesUrls,
            License license,
            JsonOperationContext context)
        {
            try
            {
                await serverStore.Engine.SetNewStateAsync(RachisState.Passive, null, serverStore.Engine.CurrentCommittedState.Term, "During setup wizard, " + "making sure there is no cluster from previous installation.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
            }

            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePath), out string certificateFileName);

            serverStore.Server.Certificate = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup",
                serverCert,
                serverCertBytes,
                certPassword,
                serverStore.GetLicenseType(),
                true);

            if (continueSetupInfo.NodeTag.Equals(firstNodeTag))
            {
                await serverStore.EnsureNotPassiveAsync(publicServerUrl, firstNodeTag);

                await DeleteAllExistingCertificates(serverStore);

                if (setupMode == SetupMode.LetsEncrypt && license != null)
                {
                    await serverStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
                    await serverStore.LicenseManager.ActivateAsync(license, RaftIdGenerator.DontCareId);
                }

                // We already verified that leader's port is not 0, no need for it here.
                serverStore.HasFixedPort = true;

                foreach (var url in otherNodesUrls)
                {
                    progress.AddInfo($"Adding node '{url.Key}' to the cluster.");
                    onProgress(progress);

                    try
                    {
                        await serverStore.AddNodeToClusterAsync(url.Value, url.Key, validateNotInTopology: false);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to add node '{continueSetupInfo.NodeTag}' to the cluster.", e);
                    }
                }
            }

            progress.AddInfo("Registering client certificate in the local server.");
            onProgress(progress);
            var certDef = new CertificateDefinition
            {
                Name = $"{clientCert.SubjectName.Name}",
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(clientCert.Export(X509ContentType.Cert)),
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Thumbprint = clientCert.Thumbprint,
                PublicKeyPinningHash = clientCert.GetPublicKeyPinningHash(),
                NotAfter = clientCert.NotAfter,
                NotBefore = clientCert.NotBefore
            };

            try
            {
                if (continueSetupInfo.NodeTag.Equals(firstNodeTag))
                {
                    var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(clientCert.Thumbprint, certDef, RaftIdGenerator.DontCareId));
                    await serverStore.Cluster.WaitForIndexNotification(res.Index);
                }
                else
                {
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (var certificate = ctx.ReadObject(certDef.ToJson(), "Client/Certificate/Definition"))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        serverStore.Cluster.PutLocalState(ctx, clientCert.Thumbprint, certificate, certDef);
                        tx.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to register client certificate in the local server.", e);
            }

            if (continueSetupInfo.RegisterClientCert)
            {
                CertificateUtils.RegisterClientCertInOs(onProgress, progress, clientCert);
                progress.AddInfo("Registering admin client certificate in the OS personal store.");
                onProgress(progress);
            }

            var certPath = serverStore.Configuration.GetSetting(RavenConfiguration.GetKey(x => x.Core.SetupResultingServerCertificatePath)) ?? Path.Combine(AppContext.BaseDirectory, certificateFileName);

            try
            {
                progress.AddInfo($"Saving server certificate at {certPath}.");
                onProgress(progress);

                await using (var certFile = SafeFileStream.Create(certPath, FileMode.Create))
                {
                    var certBytes = serverCertBytes;
                    await certFile.WriteAsync(certBytes, 0, certBytes.Length);
                    await certFile.FlushAsync();
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to save server certificate at {certPath}.", e);
            }

            try
            {
                // During setup we use the System database to store cluster configurations as well as the trusted certificates.
                // We need to make sure that the currently used data dir will be the one written (or not written) in the resulting settings.json
                var dataDirKey = RavenConfiguration.GetKey(x => x.Core.DataDirectory);
                var currentDataDir = serverStore.Configuration.GetServerWideSetting(dataDirKey) ?? serverStore.Configuration.GetSetting(dataDirKey);
                var currentHasKey = string.IsNullOrWhiteSpace(currentDataDir) == false;

                if (currentHasKey)
                {
                    settingsJsonObject.Modifications = new DynamicJsonValue(settingsJsonObject) {[dataDirKey] = currentDataDir};
                }
                else if (settingsJsonObject.TryGet(dataDirKey, out string _))
                {
                    settingsJsonObject.Modifications = new DynamicJsonValue(settingsJsonObject);
                    settingsJsonObject.Modifications.Remove(dataDirKey);
                }

                if (settingsJsonObject.Modifications != null)
                    settingsJsonObject = context.ReadObject(settingsJsonObject, "settings.json");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to determine the data directory", e);
            }

            try
            {
                progress.AddInfo($"Saving configuration at {serverStore.Configuration.ConfigPath}.");
                onProgress(progress);

                var indentedJson = JsonStringHelper.Indent(settingsJsonObject.ToString());
                SettingsZipFileHelper.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to save configuration at {serverStore.Configuration.ConfigPath}.", e);
            }

            try
            {
                progress.Readme = SettingsZipFileHelper.CreateReadmeTextSecured(continueSetupInfo.NodeTag, publicServerUrl, false, continueSetupInfo.RegisterClientCert, false, true);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the readme text.", e);
            }
        }

        
          private static async Task CompleteUnsecuredConfigurationForNewNode(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            ContinueSetupInfo continueSetupInfo,
            BlittableJsonReaderObject settingsJsonObject,
            ServerStore serverStore,
            string firstNodeTag,
            Dictionary<string, string> otherNodesUrls,
            License license,
            JsonOperationContext context)
        {
            try
            {
                await serverStore.Engine.SetNewStateAsync(RachisState.Passive, null, serverStore.Engine.CurrentCommittedState.Term, "During setup wizard, " + "making sure there is no cluster from previous installation.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
            }

            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
            settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);

            if (continueSetupInfo.NodeTag.Equals(firstNodeTag))
            {
                await serverStore.EnsureNotPassiveAsync(serverUrl, firstNodeTag);

                await DeleteAllExistingCertificates(serverStore);

                if (setupMode == SetupMode.Unsecured && license != null)
                {
                    await serverStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
                    await serverStore.LicenseManager.ActivateAsync(license, RaftIdGenerator.DontCareId);
                }

                // We already verified that leader's port is not 0, no need for it here.
                serverStore.HasFixedPort = true;

                foreach (var url in otherNodesUrls)
                {
                    progress.AddInfo($"Adding node '{url.Key}' to the cluster.");
                    onProgress(progress);

                    try
                    {
                        await serverStore.AddNodeToClusterAsync(url.Value, url.Key, validateNotInTopology: false);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to add node '{continueSetupInfo.NodeTag}' to the cluster.", e);
                    }
                }
            }

            try
            {
                // During setup we use the System database to store cluster configurations as well as the trusted certificates.
                // We need to make sure that the currently used data dir will be the one written (or not written) in the resulting settings.json
                var dataDirKey = RavenConfiguration.GetKey(x => x.Core.DataDirectory);
                var currentDataDir = serverStore.Configuration.GetServerWideSetting(dataDirKey) ?? serverStore.Configuration.GetSetting(dataDirKey);
                var currentHasKey = string.IsNullOrWhiteSpace(currentDataDir) == false;

                if (currentHasKey)
                {
                    settingsJsonObject.Modifications = new DynamicJsonValue(settingsJsonObject) {[dataDirKey] = currentDataDir};
                }
                else if (settingsJsonObject.TryGet(dataDirKey, out string _))
                {
                    settingsJsonObject.Modifications = new DynamicJsonValue(settingsJsonObject);
                    settingsJsonObject.Modifications.Remove(dataDirKey);
                }

                if (settingsJsonObject.Modifications != null)
                    settingsJsonObject = context.ReadObject(settingsJsonObject, "settings.json");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to determine the data directory", e);
            }

            try
            {
                progress.AddInfo($"Saving configuration at {serverStore.Configuration.ConfigPath}.");
                onProgress(progress);

                var indentedJson = JsonStringHelper.Indent(settingsJsonObject.ToString());
                SettingsZipFileHelper.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to save configuration at {serverStore.Configuration.ConfigPath}.", e);
            }

            try
            {
                progress.Readme = SettingsZipFileHelper.CreateReadmeTextUnsecured(continueSetupInfo.NodeTag, serverUrl, otherNodesUrls.Count > 0, false, true);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the readme text.", e);
            }
        }
          
        private static async Task<CompleteClusterConfigurationResult> CompleteClusterConfigurationUnsecuredSetup(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            SetupMode setupMode,
            UnsecuredSetupInfo unsecuredSetupInfo,
            ServerStore serverStore,
            CancellationToken token)
        {
            return await SetupWizardUtils.CompleteClusterConfigurationUnsecuredSetup(new CompleteClusterConfigurationParameters
            {
                OnProgress = onProgress,
                Progress = progress,
                UnsecuredSetupInfo = unsecuredSetupInfo,
                OnSettingsPath = () => serverStore.Configuration.ConfigPath,
                SetupMode = setupMode,
                OnWriteSettingsJsonLocally = indentedJson => SettingsZipFileHelper.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson),
                OnPutServerWideStudioConfigurationValues = async studioEnvironment =>
                {
                    var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(
                        new ServerWideStudioConfiguration
                        {
                            Disabled = false,
                            Environment = studioEnvironment
                        },
                        RaftIdGenerator.DontCareId));
                    await serverStore.Cluster.WaitForIndexNotification(res.Index);
                },
                OnBeforeAddingNodesToCluster = async (publicServerUrl, localNodeTag) =>
                {
                    try
                    {
                        await serverStore.Engine.SetNewStateAsync(RachisState.Passive, null, serverStore.Engine.CurrentCommittedState.Term, "During setup wizard, " + "making sure there is no cluster from previous installation.");
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
                    }

                    if (unsecuredSetupInfo.LocalNodeTag != null)
                        await serverStore.EnsureNotPassiveAsync(publicServerUrl, unsecuredSetupInfo.LocalNodeTag);
                   
                    await DeleteAllExistingCertificates(serverStore);

                    serverStore.HasFixedPort = unsecuredSetupInfo.NodeSetupInfos[localNodeTag].Port != 0;
                },
                AddNodeToCluster = async nodeTag =>
                {
                    try
                    {
                        var publicServerUrl = unsecuredSetupInfo.NodeSetupInfos[nodeTag].PublicServerUrl;
                        await serverStore.AddNodeToClusterAsync(publicServerUrl, nodeTag, validateNotInTopology: false, token: token);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to add node '{nodeTag}' to the cluster.", e);
                    }
                },
            });
        }
                
        private static async Task<CompleteClusterConfigurationResult> CompleteClusterConfigurationAndGetSettingsZipSecuredSetup(
            Action<IOperationProgress> onProgress,
            SetupProgressAndResult progress,
            SetupMode setupMode,
            SetupInfo setupInfo,
            ServerStore serverStore,
            CancellationToken token)
        {
            return await SetupWizardUtils.CompleteClusterConfigurationSecuredSetup(new CompleteClusterConfigurationParameters
            {
                OnProgress = onProgress,
                Progress = progress,
                SetupInfo = setupInfo,
                OnSettingsPath = () => serverStore.Configuration.ConfigPath,
                SetupMode = setupMode,
                OnWriteSettingsJsonLocally = indentedJson => SettingsZipFileHelper.WriteSettingsJsonLocally(serverStore.Configuration.ConfigPath, indentedJson),
                OnGetCertificatePath = certificateFileName =>
                {
                    return serverStore.Configuration.GetSetting(RavenConfiguration.GetKey(x => x.Core.SetupResultingServerCertificatePath)) ??
                           Path.Combine(AppContext.BaseDirectory, certificateFileName);
                },
                OnPutServerWideStudioConfigurationValues = async studioEnvironment =>
                    {
                    var res = await serverStore.PutValueInClusterAsync(new PutServerWideStudioConfigurationCommand(
                        new ServerWideStudioConfiguration {Disabled = false, Environment = studioEnvironment}, RaftIdGenerator.DontCareId));

                    await serverStore.Cluster.WaitForIndexNotification(res.Index);
                },
                OnBeforeAddingNodesToCluster = async (publicServerUrl, localNodeTag) =>
                        {
                            try
                            {
                                await serverStore.Engine.SetNewStateAsync(RachisState.Passive, null, serverStore.Engine.CurrentCommittedState.Term, "During setup wizard, " + "making sure there is no cluster from previous installation.");
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException("Failed to delete previous cluster topology during setup.", e);
                            }

                            await serverStore.EnsureNotPassiveAsync(publicServerUrl, setupInfo.LocalNodeTag);

                            await DeleteAllExistingCertificates(serverStore);

                            if (setupMode == SetupMode.LetsEncrypt)
                            {
                                await serverStore.EnsureNotPassiveAsync(skipLicenseActivation: true);
                                await serverStore.LicenseManager.ActivateAsync(setupInfo.License, RaftIdGenerator.DontCareId);
                            }

                            serverStore.HasFixedPort = setupInfo.NodeSetupInfos[localNodeTag].Port != 0;
                },
                PutCertificateInCluster = async (selfSignedCertificate, newCertDef) =>
                            {
                                try
                                {
                        var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(selfSignedCertificate.Thumbprint, newCertDef, RaftIdGenerator.DontCareId));
                        await serverStore.Cluster.WaitForIndexNotification(res.Index);
                                }
                                catch (Exception e)
                                {
                        throw new InvalidOperationException($"Failed to to put certificate in cluster. self signed certificate thumbprint'{selfSignedCertificate.Thumbprint}'.", e);
                                }
                },
                AddNodeToCluster = async nodeTag =>
                        {
                        try
                        {
                        await serverStore.AddNodeToClusterAsync(setupInfo.NodeSetupInfos[nodeTag].PublicServerUrl, nodeTag, validateNotInTopology: false, token: token);
                        }
                        catch (Exception e)
                        {
                        throw new InvalidOperationException($"Failed to add node '{nodeTag}' to the cluster.", e);
                        }
                },
                RegisterClientCertInOs = (onProgressCopy, progressCopy, clientCert) => CertificateUtils.RegisterClientCertInOs(onProgressCopy, progressCopy, clientCert)
            });
                            }

        public static async Task<byte[]> GenerateCertificateTask(string name, ServerStore serverStore, SetupInfo setupInfo)
                        {
            if (serverStore.Server.Certificate?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{name}' because the server certificate is not loaded.");

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(name, serverStore.Server.Certificate, out var certBytes,
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
                NotAfter = selfSignedCertificate.NotAfter,
                NotBefore = selfSignedCertificate.NotBefore
            };

            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(selfSignedCertificate.Thumbprint, newCertDef, RaftIdGenerator.DontCareId));
                            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            return certBytes;
                        }
        internal static async Task DeleteAllExistingCertificates(ServerStore serverStore)
                        {
            // If a user repeats the setup process, there might be certificate leftovers in the cluster

            List<string> existingCertificateKeys;
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                            {
                existingCertificateKeys = serverStore.Cluster.GetCertificateThumbprintsFromCluster(context).ToList();
                            }

            if (existingCertificateKeys.Count == 0)
                return;

            var res = await serverStore.SendToLeaderAsync(new DeleteCertificateCollectionFromClusterCommand(RaftIdGenerator.NewId()) {Names = existingCertificateKeys});

            await serverStore.Cluster.WaitForIndexNotification(res.Index);
                                }

        public static BlittableJsonReaderObject ExtractCertificatesAndSettingsJsonFromZip(byte[] zipBytes, string currentNodeTag, JsonOperationContext context,
            out byte[] certBytes, out X509Certificate2 serverCert, out X509Certificate2 clientCert, out string firstNodeTag,
            out Dictionary<string, string> otherNodesUrls, out License license, bool isSecured = true)
        {
            certBytes = null;
            serverCert = null;
            clientCert = null;
            byte[] clientCertBytes = null;
            BlittableJsonReaderObject currentNodeSettingsJson = null;
            license = null;
            otherNodesUrls = new Dictionary<string, string>();

            firstNodeTag = "A";

            using (var msZip = new MemoryStream(zipBytes))
            using (var archive = new ZipArchive(msZip, ZipArchiveMode.Read, false))
                                {
                foreach (var entry in archive.Entries)
                            {
                    // try to find setup.json file first, as we make decisions based on its contents
                    if (entry.Name.Equals("setup.json"))
                        {
                        var json = context.Sync.ReadForMemory(entry.Open(), "license/json");

                        SetupSettings setupSettings = JsonDeserializationServer.SetupSettings(json);
                        firstNodeTag = setupSettings.Nodes[0].Tag;

                        // Since we allow to customize node tags, we stored information about the order of nodes into setup.json file
                        // The first node is the one in which the cluster should be initialized.
                        // If the file isn't found, it means we are using a zip which was created in the old codebase => first node has the tag 'A'
                            }
                        }

                foreach (var entry in archive.Entries)
        {
                    if (entry.FullName.StartsWith($"{currentNodeTag}/") && entry.Name.EndsWith(".pfx"))
        {
                        using (var ms = new MemoryStream())
        {
                            entry.Open().CopyTo(ms);
                            certBytes = ms.ToArray();
                }
                }

                    if (entry.Name.StartsWith("admin.client.certificate") && entry.Name.EndsWith(".pfx"))
        {
                        using (var ms = new MemoryStream())
            {
                            entry.Open().CopyTo(ms);
                            clientCertBytes = ms.ToArray();
            }
            }

                    if (entry.Name.Equals("license.json"))
            {
                        var json = context.Sync.ReadForMemory(entry.Open(), "license/json");
                        license = JsonDeserializationServer.License(json);
            }

                    if (entry.Name.Equals("settings.json"))
        {
                        using (var settingsJson = context.Sync.ReadForMemory(entry.Open(), "settings-json-from-zip"))
            {
                            settingsJson.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string publicServerUrl);
                            settingsJson.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);

                            if (entry.FullName.StartsWith($"{currentNodeTag}/"))
            {
                                currentNodeSettingsJson = settingsJson.Clone(context);
            }

                            // This is for the case where we take the zip file and use it to setup the first node as well.
                            // If this is the first node, we must collect the urls of the other nodes so that
                            // we will be able to add them to the cluster when we bootstrap the cluster.
                            if (entry.FullName.StartsWith(firstNodeTag + "/") == false)
                            {
                                var tag = entry.FullName.Substring(0, entry.FullName.Length - "/settings.json".Length);
                                otherNodesUrls.Add(tag, publicServerUrl ?? serverUrl);
                            }
                        }
                    }
                }
            }

            if (certBytes == null && isSecured)
                throw new InvalidOperationException($"Could not extract the server certificate of node '{currentNodeTag}'. Are you using the correct zip file?");
            if (clientCertBytes == null && isSecured)
                throw new InvalidOperationException("Could not extract the client certificate. Are you using the correct zip file?");
            if (currentNodeSettingsJson == null)
                throw new InvalidOperationException($"Could not extract settings.json of node '{currentNodeTag}'. Are you using the correct zip file?");

            try
            {
                if (isSecured)
                {
                    currentNodeSettingsJson.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
                    serverCert = CertificateLoaderUtil.CreateCertificate(certBytes, certPassword, CertificateLoaderUtil.FlagsForPersist);   
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to load the server certificate of node '{currentNodeTag}'.", e);
                            }

            try
            {
                if (isSecured)
                {
                    clientCert = CertificateLoaderUtil.CreateCertificate(clientCertBytes, flags: CertificateLoaderUtil.FlagsForPersist);
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Unable to load the client certificate.", e);
                }

            return currentNodeSettingsJson;
            }
        }
        }
