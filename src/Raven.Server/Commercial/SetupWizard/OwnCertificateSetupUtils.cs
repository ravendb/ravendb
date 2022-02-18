using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Commercial.LetsEncrypt;

namespace Raven.Server.Commercial.SetupWizard;

public class OwnCertificateSetupUtils
{
    public static async Task<byte[]> Setup(SetupInfo setupInfo, SetupProgressAndResult progress, CancellationToken token)
    {
        try
        {
            progress.Processed++;
            progress.AddInfo("Setting up RavenDB in 'Secured Mode'.");
            progress.AddInfo("Starting validation.");

            if (EmailValidator.IsValid(setupInfo.Email) == false)
                throw new ArgumentException("Invalid e-mail format: " + setupInfo.Email);

            try
            {
                var completeClusterConfigurationResult = await SetupWizardUtils.CompleteClusterConfiguration(new CompleteClusterConfigurationParameters
                {
                    Progress = progress,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.None,
                    LicenseType = LicenseType.None,
                    Token = CancellationToken.None,
                    CertificateValidationKeyUsages = true
                });

                var zipFile = await SettingsZipFileHelper.GetSetupZipFile(new GetSetupZipFileParameters
                {
                    CompleteClusterConfigurationResult = completeClusterConfigurationResult,
                    Progress = progress,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.None,
                    Token = CancellationToken.None,
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
        catch (Exception e)
        {
            const string str = "Setting up RavenDB in 'Secured Mode' failed.";
            progress?.AddError(str, e);
            throw new InvalidOperationException(str, e);
        }
    }

}
