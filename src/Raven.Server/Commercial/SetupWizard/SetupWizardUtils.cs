using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Server.Commercial;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace rvn.Server.SetupWizard;

public class SetupWizardUtils
{ 
    public static async Task<CompleteClusterConfigurationResult> CompleteClusterConfiguration(CompleteClusterConfigurationParameters parameters)
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
                serverCert = new X509Certificate2(serverCertBytes, parameters.SetupInfo.Password, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

                var localNodeTag = parameters.SetupInfo.LocalNodeTag;
                publicServerUrl = CertificateUtils.GetServerUrlFromCertificate(serverCert,
                    parameters.SetupInfo,
                    localNodeTag,
                    parameters.SetupInfo.NodeSetupInfos[localNodeTag].Port,
                    parameters.SetupInfo.NodeSetupInfos[localNodeTag].TcpPort,
                    out _,
                    out domainFromCert);

                if (parameters.OnBeforeAddingNodesToCluster != null)
                    await parameters.OnBeforeAddingNodesToCluster(publicServerUrl, localNodeTag);

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

                serverCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder("Setup", serverCert, serverCertBytes,
                    parameters.SetupInfo.Password, parameters.LicenseType, parameters.CertificateValidationKeyUsages, parameters.Progress);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not load the certificate in the local server.", e);
            }

            parameters.Progress?.AddInfo("Generating the client certificate.");
            parameters.OnProgress?.Invoke(parameters.Progress);

            X509Certificate2 clientCert;

            var domain = (parameters.SetupMode == SetupMode.Secured)
                ? domainFromCert.ToLower()
                : parameters.SetupInfo.Domain.ToLower();

            byte[] certBytes;
            try
            {
                // requires server certificate to be loaded
                var clientCertificateName = $"{domain}.client.certificate";
                var result = LetsEncryptCertificateUtil.GenerateCertificate(serverCertificateHolder, clientCertificateName, parameters.SetupInfo);
                certBytes = result.CertBytes;

                if (parameters.PutCertificate != null)
                    await parameters.PutCertificate(result.CertificateDefinition);

                clientCert = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.MachineKeySet);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not generate a client certificate for '{domain}'.", e);
            }

            parameters.RegisterClientCertInOs?.Invoke(parameters.OnProgress, parameters.Progress, clientCert);

            return new CompleteClusterConfigurationResult
            {
                Domain = domain,
                CertBytes = certBytes,
                ServerCertBytes = serverCertBytes,
                ServerCert = serverCertificateHolder.Certificate,
                PublicServerUrl = publicServerUrl,
                ClientCert = clientCert
            };
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Failed to create settings file(s).", e);
        }
    }
}
