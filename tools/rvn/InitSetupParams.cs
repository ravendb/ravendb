using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Commercial;

namespace rvn;

public class InitSetupParams
{
    public static async Task RunAsync(string outputFilePath, string setupMode, CancellationToken token)
    {
        SetupInfoBase setupInfoLetsEncryptSkeleton = GenerateSetupInfo(setupMode);

        var json = JsonConvert.SerializeObject(setupInfoLetsEncryptSkeleton, Formatting.Indented, new JsonSerializerSettings()
        {
            NullValueHandling = NullValueHandling.Ignore,
            DefaultValueHandling = DefaultValueHandling.Ignore
        });
                    
        await File.WriteAllTextAsync(outputFilePath!, json, token); 
    }

    private static SetupInfoBase GenerateSetupInfo(string mode)
    {
        switch (mode)
        {
            case CommandLineApp.LetsEncrypt:
                return GenerateSetupInfoForLetsEncrypt();
            case CommandLineApp.OwnCertificate:
                return GenerateSetupInfoForOwnCert();
            case CommandLineApp.Unsecured:
                return GenerateSetupInfoForUnsecured();
            default:
                throw new NotSupportedException();
        }
    }

    private static SetupInfoBase GenerateSetupInfoForOwnCert()
    {
        return new SetupInfo()
        {
            Domain = "subdomain",
            RootDomain = "example.com",
            License = new License() { Id = Guid.Empty, Keys = new List<string>() { string.Empty }, Name = string.Empty },
            NodeSetupInfos = new Dictionary<string, NodeInfo>()
            {
                {
                    "A",
                    new NodeInfo()
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

    private static SetupInfoBase GenerateSetupInfoForLetsEncrypt()
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
            NodeSetupInfos = new Dictionary<string, NodeInfo>()
            {
                {
                    "A",
                    new NodeInfo()
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

    private static SetupInfoBase GenerateSetupInfoForUnsecured()
    {
        return new UnsecuredSetupInfo()
        {
            Environment = StudioConfiguration.StudioEnvironment.Development,
            LocalNodeTag = "A",
            NodeSetupInfos = new Dictionary<string, NodeInfo>()
            {
                {
                    "A",
                    new NodeInfo()
                    {
                        Addresses = new List<string>() {"http://0.0.0.0:0"},
                        Port = 443,
                        TcpPort = 38888,
                    }
                }
            }
        };
    }
}

