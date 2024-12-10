using System;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

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
            string domainFromCert;
            string publicServerUrl;
            CertificateUtils.CertificateHolder serverCertificateHolder;

            try
            {
                var base64 = parameters.SetupInfo.Certificate;
                serverCertBytes = Convert.FromBase64String(base64);
                serverCert = CertificateLoaderUtil.CreateCertificateFromPfx(serverCertBytes, parameters.SetupInfo.Password, CertificateLoaderUtil.FlagsForExport);

                var localNodeTag = parameters.SetupInfo.LocalNodeTag ?? parameters.SetupInfo.NodeSetupInfos.Keys.FirstOrDefault();
                if (localNodeTag is null)
                {
                    throw new InvalidOperationException($"Could not determine {nameof(localNodeTag)}");
                }

                publicServerUrl = CertificateUtils.GetServerUrlFromCertificate(serverCert,
                    parameters.SetupInfo,
                    localNodeTag,
                    parameters.SetupInfo.NodeSetupInfos[localNodeTag].Port,
                    parameters.SetupInfo.NodeSetupInfos[localNodeTag].TcpPort,
                    out _,
                    out domainFromCert);

                if (parameters.OnBeforeAddingNodesToCluster != null && parameters.SetupInfo.ZipOnly == false)
                        await parameters.OnBeforeAddingNodesToCluster(publicServerUrl, localNodeTag);

                serverCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup",
                    serverCert,
                    serverCertBytes,
                    parameters.SetupInfo.Password,
                    parameters.LicenseType,
                    parameters.CertificateValidationKeyUsages,
                    parameters.Progress);

                if(parameters.SetupInfo.ZipOnly == false)
                {
                    foreach (var node in parameters.SetupInfo.NodeSetupInfos)
                    {
                        if (node.Key == parameters.SetupInfo.LocalNodeTag)
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
            parameters.OnProgress?.Invoke(parameters.Progress);

            X509Certificate2 clientCert;

            var domain = (parameters.SetupMode == SetupMode.Secured)
                ? domainFromCert.ToLower()
                : parameters.SetupInfo.Domain.ToLower();

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

                if (parameters.PutCertificateInCluster != null && parameters.SetupInfo.RegisterClientCert && parameters.SetupInfo.ZipOnly == false)
                    await parameters.PutCertificateInCluster(selfSignedCertificate, certificateDefinition);

                clientCert = CertificateLoaderUtil.CreateCertificateFromPfx(certBytes, flags: CertificateLoaderUtil.FlagsForPersist);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to generate a client certificate for '{domain}'.", e);
            }

            if (parameters.SetupInfo.RegisterClientCert)
                parameters.RegisterClientCertInOs?.Invoke(parameters.OnProgress, parameters.Progress, clientCert);

            return new CompleteClusterConfigurationResult
            {
                Domain = domain,
                CertBytes = certBytes,
                ServerCertBytes = serverCertBytes,
                ServerCert = serverCertificateHolder.Certificate,
                PublicServerUrl = publicServerUrl,
                ClientCert = clientCert,
                CertificateDefinition = certificateDefinition
            };
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to create settings file(s).", e);
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
        
        (string localNodeTag, NodeInfo nodeInfo) = nodeSetupInfos.First();

        try
        {
                if (parameters.OnBeforeAddingNodesToCluster != null && zipOnly == false )
                    await parameters.OnBeforeAddingNodesToCluster(nodeInfo.PublicServerUrl, localNodeTag);
                
                if (zipOnly == false)
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
