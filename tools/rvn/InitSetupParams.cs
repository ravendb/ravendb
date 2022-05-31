using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Raven.Server.Commercial;

namespace rvn;

public class InitSetupParams
{
    public static async Task RunAsync(string outputFilePath, string setupMode, CancellationToken token)
    {
        SetupInfo setupInfoLetsEncryptSkel = GenerateSetupInfo(setupMode);

        var json = JsonConvert.SerializeObject(setupInfoLetsEncryptSkel, Formatting.Indented, new JsonSerializerSettings()
        {
            NullValueHandling = NullValueHandling.Ignore,
            DefaultValueHandling = DefaultValueHandling.Ignore
        });
                    
        await File.WriteAllTextAsync(outputFilePath!, json, token); 
    }

    private static SetupInfo GenerateSetupInfo(string mode)
    {
        switch (mode)
        {
            case CommandLineApp.LetsEncrypt:
                return GenerateSetupInfoForLetsEncrypt();
            case CommandLineApp.OwnCertificate:
                return GenerateSetupInfoForOwnCert();
            default:
                throw new NotSupportedException();
        }
    }

    private static SetupInfo GenerateSetupInfoForOwnCert()
    {
        return GenerateTemplateSetupInfo("subdomain", "example.com");
    }

    private static SetupInfo GenerateSetupInfoForLetsEncrypt()
    {
        var setupInfo = GenerateTemplateSetupInfo("your-domain", "development.run");
        setupInfo.Email = string.Empty;
        return setupInfo;
    }

    private static SetupInfo GenerateTemplateSetupInfo(string domain, string rootDomain)
    {
        return new SetupInfo()
        {
            Domain = domain,
            RootDomain = rootDomain,
            License = new License() { Id = new Guid(), Keys = new List<string>() { string.Empty }, Name = string.Empty },
            NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
            {
                {
                    "A",
                    new SetupInfo.NodeInfo()
                    {
                        Addresses = new List<string>() { "https://0.0.0.0:0" },
                        Port = 443,
                        TcpPort = 38888,
                        PublicServerUrl = $"https://{domain}.{rootDomain}",
                        PublicTcpServerUrl = $"tcp://{domain}.{rootDomain}:38888"
                    }
                }
            }
        };
    }
}
