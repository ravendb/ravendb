using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Commercial.LetsEncrypt;

namespace Raven.Server.Commercial.SetupWizard;

public class UnsecuredSetupUtils
{
    public static async Task<byte[]> Setup(UnsecuredSetupInfo unsecuredSetupInfo, SetupProgressAndResult progress, CancellationToken token)
    {
        try
        {
            progress.AddInfo("Setting up RavenDB in 'Unsecured Mode'.");
            progress.AddInfo("Starting validation.");
            try
            {
                
                foreach (var node in unsecuredSetupInfo.NodeSetupInfos.Values)
                {
                    node.PublicServerUrl = string.Join(";", node.Addresses.Select(ip => SettingsZipFileHelper.IpAddressToUrl(ip, node.Port, scheme: "http")));
                }

                var completeClusterConfigurationResult = await SetupWizardUtils.CompleteClusterConfigurationUnsecuredSetup(new CompleteClusterConfigurationParameters
                {
                    Progress = progress,
                    UnsecuredSetupInfo = unsecuredSetupInfo,
                    SetupMode = SetupMode.Unsecured,
                    Token = token
                });

                progress.SettingsZipFile = await SettingsZipFileHelper.GetSetupZipFileUnsecuredSetup(new GetSetupZipFileParameters
                {
                    CompleteClusterConfigurationResult = completeClusterConfigurationResult,
                    Progress = progress,
                    UnsecuredSetupInfo = unsecuredSetupInfo,
                    SetupMode = SetupMode.Unsecured,
                    Token = token,
                });
                
                progress.Processed++;
                progress.AddInfo("Configuration settings created.");
                progress.AddInfo("Setting up RavenDB in 'Unsecured Mode' finished successfully.");

                return progress.SettingsZipFile;
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not create configuration settings.", e);
            }

        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Setting up RavenDB in 'Unsecured Mode' failed.", e);
        }
    }
}
