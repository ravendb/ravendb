using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Raven.Server.Commercial.LetsEncrypt
{
    public static class LetsEncryptRvnUtils
    {
        private const string AcmeClientUrl = "https://acme-v02.api.letsencrypt.org/directory";
        private const string DnsAction = "claim";

        public static async Task<byte[]> SetupLetsEncrypt(SetupInfo setupInfo, string settingsPath, SetupProgressAndResult progress, string dataFolder, CancellationToken token)
        {
            progress?.AddInfo("Setting up RavenDB in Let's Encrypt security mode.");

            if (ZipFileHelper.IsValidEmail(setupInfo.Email) == false)
                throw new ArgumentException("Invalid e-mail format" + setupInfo.Email);

            var acmeClient = new LetsEncryptClient(AcmeClientUrl);

            await acmeClient.Init(setupInfo.Email, token);
            progress?.AddInfo($"Getting challenge(s) from Let's Encrypt. Using e-mail: {setupInfo.Email}.");

            (string Challenge, LetsEncryptClient.CachedCertificateResult Cache) challengeResult;
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
                    var response = await ApiHttpClient.Instance.PostAsync($"/api/v1/dns-n-cert/{DnsAction}", content, CancellationToken.None).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to perform the given action: {DnsAction}", e);
                }
           
                await RavenDnsRecordHelper.UpdateDnsRecordsTask(new RavenDnsRecordHelper.UpdateDnsRecordParameters
                {
                    Challenge = challengeResult.Challenge,
                    SetupInfo = setupInfo,
                    Progress = progress,
                    Token = CancellationToken.None
                });
                progress?.AddInfo($"Updating DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to update DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}", e);
            }

            progress?.AddInfo($"Successfully updated DNS record(s) and challenge(s) in {setupInfo.Domain.ToLower()}.{setupInfo.RootDomain.ToLower()}");
            progress?.AddInfo("Completing Let's Encrypt challenge(s)...");

            await ZipFileHelper.CompleteAuthorizationAndGetCertificate(new ZipFileHelper.CompleteAuthorizationAndGetCertificateParameters
            {
                OnValidationSuccessful = () =>
                {
                    progress?.AddInfo("Let's Encrypt challenge(s) completed successfully.");
                    progress?.AddInfo("Acquiring certificate.");
                },
                SetupInfo = setupInfo,
                Client = acmeClient,
                ChallengeResult = challengeResult,
                Token = CancellationToken.None,
            });

            progress?.AddInfo("Successfully acquired certificate from Let's Encrypt.");
            progress?.AddInfo("Starting validation.");
            
            Func<string, Task<string>> onCertPathFunc = null;
            try
            {
                if (string.IsNullOrEmpty(dataFolder) == false)
                {
                    onCertPathFunc = (getCertificatePath) => Task.Run(() => Path.Combine(dataFolder, getCertificatePath), token);
                }
                
                var zipFile = await ZipFileHelper.CompleteClusterConfigurationAndGetSettingsZip(new ZipFileHelper.CompleteClusterConfigurationParameters
                {
                    Progress = progress,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.None,
                    SettingsPath = settingsPath,
                    OnGetCertificatePath = onCertPathFunc,
                    LicenseType = LicenseType.None,
                    Token = CancellationToken.None,
                    CertificateValidationKeyUsages = true
                });

                return zipFile;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to create the configuration settings.", e);
            }
        }

        public static async Task<byte[]> ImportCertificateSetup(SetupInfo setupInfo, string settingsPath, SetupProgressAndResult progress, CancellationToken token)
        {
            try
            {
                
                progress?.AddInfo("Setting up RavenDB in 'Secured Mode'.");
                progress?.AddInfo("Starting validation.");

                if (ZipFileHelper.IsValidEmail(setupInfo.Email) == false)
                    throw new ArgumentException("Invalid e-mail format: " + setupInfo.Email);

                byte[] zipFile;
                try
                {
                     zipFile = await ZipFileHelper.CompleteClusterConfigurationAndGetSettingsZip(new ZipFileHelper.CompleteClusterConfigurationParameters
                    {
                        Progress = progress,
                        SetupInfo = setupInfo,
                        SetupMode = SetupMode.None,
                        SettingsPath = settingsPath,
                        LicenseType = LicenseType.None,
                        Token = CancellationToken.None,
                        CertificateValidationKeyUsages = true
                    });

                     if (progress != null)
                     {
                         progress.Processed++;
                         progress.AddInfo("Configuration settings created.");
                         progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
                     }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Failed to create the configuration settings.", e);
                }

                return zipFile;
            }
            catch (Exception e)
            {
                const string str = "Setting up RavenDB in 'Secured Mode' failed.";
                progress?.AddError(str, e);
                throw new InvalidOperationException(str, e);
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
    }
}
