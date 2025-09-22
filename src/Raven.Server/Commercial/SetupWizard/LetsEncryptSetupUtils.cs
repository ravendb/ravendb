using System;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.Utils;

namespace Raven.Server.Commercial.SetupWizard;

public static class LetsEncryptSetupUtils
{
        public static async Task<byte[]> Setup(SetupInfo setupInfo,  SetupProgressAndResult progress, bool registerTcpDnsRecords, string acmeUrl, CancellationToken token)
        {
            progress.Processed++;
            progress?.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");
            
            if (EmailValidator.IsValid(setupInfo.Email) == false)
                throw new ArgumentException("Invalid e-mail format" + setupInfo.Email);
            
            var acmeClient = new LetsEncryptClient(acmeUrl);

            await acmeClient.Init(setupInfo.Email, token);

            progress.Processed++;
            progress?.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            (string Challenge, LetsEncryptClient.CachedCertificateResult CachedCertificateResult) challengeResult;
            try
            {
                challengeResult = await InitialLetsEncryptChallenge(setupInfo, acmeClient, token);

                progress?.AddInfo(challengeResult.Challenge != null
                    ? "Successfully received challenge(s) information from Let's Encrypt."
                    : "Using cached Let's Encrypt certificate.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to initialize lets encrypt challenge", e);
            }

            try
            {
                var registrationInfo = new RegistrationInfo
                {
                    License = setupInfo.License,
                    Domain = setupInfo.Domain,
                    Challenge = challengeResult.Challenge,
                    RootDomain = setupInfo.RootDomain,
                };
                var serializeObject = JsonConvert.SerializeObject(registrationInfo);

                try
                {
                    var content = new StringContent(serializeObject, Encoding.UTF8, "application/json");
                    var response = await ApiHttpClient.PostAsync($"/api/v1/dns-n-cert/claim", content, token).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                    progress?.AddInfo($"Successfully claimed this domain: {setupInfo.Domain}.");
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to claim the given domain: {registrationInfo.Domain}", e);
                }
                await RavenDnsRecordHelper.UpdateDnsRecordsTask(new UpdateDnsRecordParameters
                {
                    Challenge = challengeResult.Challenge,
                    SetupInfo = setupInfo,
                    Progress = progress,
                    Token = token,
                    RegisterTcpDnsRecords = registerTcpDnsRecords
                });
                progress?.AddInfo($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
            }

            progress.Processed++;
            progress?.AddInfo($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");
            progress?.AddInfo("Completing Let's Encrypt challenge(s)...");

           await CertificateUtils.CompleteAuthorizationAndGetCertificate(new CompleteAuthorizationAndGetCertificateParameters
            {
                OnValidationSuccessful = () =>
                {
                    progress?.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                    progress?.AddInfo("Acquiring certificate.");
                    progress.Processed++;
                },
                SetupInfo = setupInfo,
                Client = acmeClient,
                ChallengeResult = challengeResult,
                Token = token
            });

            progress.Processed++;
            progress?.AddInfo("Successfully acquired certificate from Let's Encrypt.");
            progress?.AddInfo("Starting validation.");
            
            try
            {
                var completeClusterConfigurationResult = await SetupWizardUtils.CompleteClusterConfigurationSecuredSetup(new CompleteClusterConfigurationParameters
                {
                    Progress = progress,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.None,
                    LicenseType = LicenseType.None,
                    Token = token,
                    CertificateValidationKeyUsages = true,
                });

                var zipFile = await SettingsZipFileHelper.GetSetupZipFileSecuredSetup(new GetSetupZipFileParameters
                {
                    CompleteClusterConfigurationResult = completeClusterConfigurationResult,
                    Progress = progress,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.LetsEncrypt,
                    Token = token,
                });
                
                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                
                return zipFile;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the configuration settings.", e);
            }
        }

        public static async Task<(string Challenge, LetsEncryptClient.CachedCertificateResult CachedCertificateResult)> InitialLetsEncryptChallenge(
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
}
