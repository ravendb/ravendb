using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Commercial;
using Raven.Server.Commercial.LetsEncrypt;

namespace rvn.Server.SetupWizard;

public class ImportCertificateSetupUtils
{
    public static async Task<byte[]> Setup(SetupInfo setupInfo, SetupProgressAndResult progress, CancellationToken token)
    {
        try
        {
            progress.Processed++;
            progress?.AddInfo("Setting up RavenDB in 'Secured Mode'.");
            progress?.AddInfo("Starting validation.");

            if (EmailValidator.IsValid(setupInfo.Email) == false)
                throw new ArgumentException("Invalid e-mail format: " + setupInfo.Email);

            byte[] zipFile;
            try
            {
                zipFile = await SettingsZipFileHelper.CompleteClusterConfigurationAndGetSettingsZip(new CompleteClusterConfigurationParameters
                {
                    Progress = progress,
                    SetupInfo = setupInfo,
                    SetupMode = SetupMode.None,
                    LicenseType = LicenseType.None,
                    Token = CancellationToken.None,
                    CertificateValidationKeyUsages = true
                });
                     
                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in 'Secured Mode' finished successfully.");
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

}
