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
        return new SetupInfo()
        {
            Domain = "subdomain",
            RootDomain = "example.com",
            License = new License() { Id = Guid.Empty, Keys = new List<string>() { string.Empty }, Name = string.Empty },
            NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
            {
                {
                    "A",
                    new SetupInfo.NodeInfo()
                    {
                        Addresses = new List<string>() { "https://0.0.0.0:0" },
                        Port = 443,
                        TcpPort = 38888,
                        PublicServerUrl = "https://subdomain.example.com",
                        PublicTcpServerUrl = "tcp://subdomain.example.com:38888"
                    }
                }
            }
        };
    }

    private static SetupInfo GenerateSetupInfoForLetsEncrypt()
    {
        return new SetupInfo()
        {
            Domain = "your-domain",
            RootDomain = "development.run",
            Email = string.Empty,
            License = new License()
            {
                Id = new Guid(), 
                Keys = new List<string>() { string.Empty }, 
                Name = string.Empty
            },
            NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
            {
                {
                    "A",
                    new SetupInfo.NodeInfo()
                    {
                        Addresses = new List<string>() { "https://0.0.0.0:0" },
                        Port = 443,
                        TcpPort = 38888,
                        PublicServerUrl = "https://your-domain.development.run",
                        PublicTcpServerUrl = "tcp://your-domain.development.run:38888"
                    }
                }
            }
        };
    }
}
