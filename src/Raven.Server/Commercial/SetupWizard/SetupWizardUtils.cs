using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Commercial.SetupWizard;

public static class SetupWizardUtils
{
    public static async Task<CompleteClusterConfigurationResult> CompleteClusterConfigurationSecuredSetup(CompleteClusterConfigurationParameters parameters)
    {
        try
        {
            parameters.Progress?.AddInfo("Loading and validating server certificate.");
            parameters.OnProgress?.Invoke(parameters.Progress);

            byte[] serverCertBytes;
            X509Certificate2 serverCert;
            string domain;
            string domainFromCert;
            string publicServerUrl;
            CertificateUtils.CertificateHolder serverCertificateHolder;

            try
            {
                var base64 = parameters.SetupInfo.Certificate;
                serverCertBytes = Convert.FromBase64String(base64);
                serverCert = CertificateLoaderUtil.CreateCertificate(serverCertBytes, parameters.SetupInfo.Password, CertificateLoaderUtil.FlagsForExport);

                var localNodeTag = parameters.SetupInfo.LocalNodeTag;
                if (localNodeTag is null)
                {
                    throw new InvalidOperationException($"{nameof(parameters.SetupInfo.LocalNodeTag)} must be set");
                }

                publicServerUrl = CertificateUtils.GetServerUrlFromCertificate(serverCert,
                    parameters.SetupInfo,
                    localNodeTag,
                    parameters.SetupInfo.NodeSetupInfos[localNodeTag].Port,
                    parameters.SetupInfo.NodeSetupInfos[localNodeTag].TcpPort,
                    out _,
                    out domainFromCert);

                domain = (parameters.SetupMode == SetupMode.Secured)
                    ? domainFromCert.ToLower()
                    : parameters.SetupInfo.Domain.ToLower();

                if (parameters.OnBeforeAddingNodesToCluster != null && parameters.SetupInfo.ZipOnly == false)
                        await parameters.OnBeforeAddingNodesToCluster(publicServerUrl, localNodeTag);

                serverCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup",
                    serverCert,
                    serverCertBytes,
                    parameters.SetupInfo.Password,
                    parameters.LicenseType,
                    parameters.CertificateValidationKeyUsages,
                    parameters.Progress);

                if (parameters.SetupInfo.ZipOnly == false && parameters.SetupInfo.StartAsPassive == false)
                {
                    await ValidateCertificateFileWriteAccess();

                    foreach (var node in parameters.SetupInfo.NodeSetupInfos)
                    {
                        if (node.Key == localNodeTag)
                            continue;

                        parameters.Progress?.AddInfo($"Adding node '{node.Key}' to the cluster.");
                        parameters.OnProgress?.Invoke(parameters.Progress);

                        parameters.SetupInfo.NodeSetupInfos[node.Key].PublicServerUrl = CertificateUtils.GetServerUrlFromCertificate(serverCert,
                            parameters.SetupInfo, node.Key,
                            node.Value.Port,
                            node.Value.TcpPort, out _, out _);

                        if (parameters.AddNodeToCluster != null)
                            await parameters.AddNodeToCluster(node.Key);
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to load and validate server certificate.", e);
            }

            parameters.Progress?.AddInfo("Generating the client certificate.");
            parameters.Progress?.SetupActionSteps.StepsByConfigurationStepType[ConfigurationStepType.ClientCertificate].SetState(State.InProgress);
            
            parameters.OnProgress?.Invoke(parameters.Progress);

            X509Certificate2 clientCert;

            byte[] certBytes;
            CertificateDefinition certificateDefinition;
            X509Certificate2 selfSignedCertificate;
            try
            {
                // requires server certificate to be loaded
                var clientCertificateName = $"{domain}.client.certificate";
                (certBytes, certificateDefinition, selfSignedCertificate) =
                    LetsEncryptCertificateUtil.GenerateClientCertificateTask(serverCertificateHolder, clientCertificateName, parameters.SetupInfo);

                Debug.Assert(selfSignedCertificate != null);

                if (parameters.PutCertificateInCluster != null && parameters.SetupInfo.RegisterClientCert && parameters.SetupInfo.ZipOnly == false && parameters.SetupInfo.StartAsPassive == false)
                    await parameters.PutCertificateInCluster(selfSignedCertificate, certificateDefinition);

                clientCert = CertificateLoaderUtil.CreateCertificate(certBytes, flags: CertificateLoaderUtil.FlagsForPersist);
            }
            catch (Exception e)
            {
                parameters.Progress?.SetupActionSteps.SetError(ConfigurationStepType.ClientCertificate, ErrorType.ClientCertificateError, e.Message);
                throw new InvalidOperationException($"Failed to generate a client certificate for '{domain}'.", e);
            }

            if (parameters.SetupInfo.RegisterClientCert)
                parameters.RegisterClientCertInOs?.Invoke(parameters.OnProgress, parameters.Progress, clientCert);
            
            parameters.Progress?.SetupActionSteps.StepsByConfigurationStepType[ConfigurationStepType.ClientCertificate].SetState(State.Completed);
            
            return new CompleteClusterConfigurationResult
            {
                Domain = domain,
                CertBytes = certBytes,
                ServerCertBytes = serverCertBytes,
                ServerCert = serverCertificateHolder.ServerCertificate,
                PublicServerUrl = publicServerUrl,
                ClientCert = clientCert,
                CertificateDefinition = certificateDefinition
            };
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to create settings file(s).", e);
        }

        async Task ValidateCertificateFileWriteAccess()
        {
            var certificateFileName = Guid.NewGuid().ToString();
            string certPath = parameters.OnGetCertificatePath?.Invoke(certificateFileName);
            if (certPath != null)
            {
                await File.WriteAllTextAsync(certPath, string.Empty);
                File.Delete(certPath);
            }
        }
    }

    public static async Task<CompleteClusterConfigurationResult> CompleteClusterConfigurationUnsecuredSetup(CompleteClusterConfigurationParameters parameters)
    {
        var zipOnly = parameters.UnsecuredSetupInfo.ZipOnly;
        var nodeSetupInfos = parameters.UnsecuredSetupInfo.NodeSetupInfos;

        parameters.Progress?.AddInfo("Completing cluster configuration.");
        parameters.OnProgress?.Invoke(parameters.Progress);

        foreach ((_, NodeInfo node) in parameters.UnsecuredSetupInfo.NodeSetupInfos)
            node.PublicServerUrl = string.Join(";", node.Addresses.Select(ip => SettingsZipFileHelper.IpAddressToUrl(ip, node.Port, scheme: "http")));

        var localNodeTag = parameters.UnsecuredSetupInfo.LocalNodeTag;
        if (localNodeTag is null)
            throw new InvalidOperationException($"{nameof(parameters.UnsecuredSetupInfo.LocalNodeTag)} must be set");

        var nodeInfo = nodeSetupInfos[localNodeTag];

        try
        {
            if (parameters.OnBeforeAddingNodesToCluster != null && zipOnly == false )
                await parameters.OnBeforeAddingNodesToCluster(nodeInfo.PublicServerUrl, localNodeTag);

            if (zipOnly == false && parameters.UnsecuredSetupInfo.StartAsPassive == false)
            {
                foreach (var node in nodeSetupInfos)
                {
                    if (node.Key == localNodeTag)
                        continue;

                    parameters.Progress?.AddInfo($"Adding node '{node.Key}' to the cluster.");
                    parameters.OnProgress?.Invoke(parameters.Progress);

                    if (parameters.AddNodeToCluster != null)
                        await parameters.AddNodeToCluster(node.Key);
                }
            }
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed complete cluster configuration: ", e);
        }

        return new CompleteClusterConfigurationResult
        {
            PublicServerUrl = nodeInfo.PublicServerUrl
        };
    }

    public static bool IsValidNodeTag(string str)
    {
        return Regex.IsMatch(str, @"^[A-Z]{1,4}$");
    }
}
