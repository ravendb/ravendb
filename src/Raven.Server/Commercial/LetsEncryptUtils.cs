using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Org.BouncyCastle.Pkcs;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Commercial
{
    public static class LetsEncryptUtils
    {
        private const string AcmeClientUrl = "https://acme-v02.api.letsencrypt.org/directory";

        public static async Task<byte[]> SetupLetsEncryptByRvn(SetupInfo setupInfo, string settingsPath , CancellationToken token)
        {
            Console.WriteLine("Setting up RavenDB in Let's Encrypt security mode.");

            if (LetsEncryptValidationApiHelper.IsValidEmail(setupInfo.Email) == false)
                throw new ArgumentException("Invalid e-mail format" + setupInfo.Email);

            var acmeClient = new LetsEncryptClient(AcmeClientUrl);

            await acmeClient.Init(setupInfo.Email, token);
            Console.WriteLine($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            var challengeResult = await InitialLetsEncryptChallenge(setupInfo, acmeClient, token);
            Console.WriteLine(challengeResult.Challenge != null
                ? "Successfully received challenge(s) information from Let's Encrypt."
                : "Using cached Let's Encrypt certificate.");
            
            try
            {
                await RavenDnsRecordHelper.UpdateDnsRecordsTask(new RavenDnsRecordHelper.UpdateDnsRecordParameters
                {
                    Challenge = challengeResult.Challenge,
                    SetupInfo = setupInfo,
                    Token = CancellationToken.None
                });
                Console.WriteLine($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
            }

            Console.WriteLine($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");
            Console.WriteLine("Completing Let's Encrypt challenge(s)...");

            await LetsEncryptApiHelper.CompleteAuthorizationAndGetCertificate(new LetsEncryptApiHelper.CompleteAuthorizationAndGetCertificateParameters
            {
                OnValidationSuccessful = () =>
                {
                    Console.WriteLine("Successfully acquired certificate from Let's Encrypt.");
                    Console.WriteLine("Starting validation.");
                },
                SetupInfo = setupInfo,
                Client = acmeClient,
                ChallengeResult = challengeResult,
                Token = CancellationToken.None
            });

            Console.WriteLine("Successfully acquired certificate from Let's Encrypt.");
            Console.WriteLine("Starting validation.");

            try
            {
                var zipFile = await CompleteClusterConfigurationAndGetSettingsZip(new CompleteClusterConfigurationParameters
                {
                    Progress = null,
                    OnProgress = null,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.None,
                    SettingsPath = settingsPath,
                    LicenseType = LicenseType.None,
                    Token = CancellationToken.None,

                });
                
                
                return zipFile;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the configuration settings.", e);
            }
        }

       
        
    

        public static async Task<(string Challenge, LetsEncryptClient.CachedCertificateResult Cache)> InitialLetsEncryptChallenge(
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

    }
}
