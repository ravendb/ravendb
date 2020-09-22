using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Util;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Commercial
{
    public class SetupInfo
    {
        public bool EnableExperimentalFeatures { get; set; }
        public StudioConfiguration.StudioEnvironment Environment { get; set; }
        public bool RegisterClientCert { get; set; }
        public DateTime? ClientCertNotAfter { get; set; }
        public License License { get; set; }
        public string Email { get; set; }
        public string Domain { get; set; }
        public string RootDomain { get; set; }
        public bool ModifyLocalServer { get; set; }
        public string LocalNodeTag { get; set; }
        public string Certificate { get; set; }
        public string Password { get; set; }

        public Dictionary<string, NodeInfo> NodeSetupInfos { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(EnableExperimentalFeatures)] = EnableExperimentalFeatures,
                [nameof(Environment)] = Environment,
                [nameof(License)] = License.ToJson(),
                [nameof(Email)] = Email,
                [nameof(Domain)] = Domain,
                [nameof(RootDomain)] = RootDomain,
                [nameof(ModifyLocalServer)] = ModifyLocalServer,
                [nameof(RegisterClientCert)] = RegisterClientCert,
                [nameof(ClientCertNotAfter)] = ClientCertNotAfter,
                [nameof(Certificate)] = Certificate,
                [nameof(Password)] = Password,
                [nameof(NodeSetupInfos)] = DynamicJsonValue.Convert(NodeSetupInfos)
            };
        }

        public class NodeInfo
        {
            public string PublicServerUrl { get; set; }
            public string PublicTcpServerUrl { get; set; }
            public int Port { get; set; }
            public int TcpPort { get; set; }
            public string ExternalIpAddress { get; set; }
            public int ExternalPort { get; set; }
            public int ExternalTcpPort { get; set; }
            public List<string> Addresses { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(PublicServerUrl)] = PublicServerUrl,
                    [nameof(PublicTcpServerUrl)] = PublicTcpServerUrl,
                    [nameof(Port)] = Port,
                    [nameof(TcpPort)] = TcpPort,
                    [nameof(ExternalIpAddress)] = ExternalIpAddress,
                    [nameof(ExternalPort)] = ExternalPort,
                    [nameof(ExternalTcpPort)] = ExternalTcpPort,
                    [nameof(Addresses)] = new DynamicJsonArray(Addresses)
                };
            }
        }

        public X509Certificate2 GetX509Certificate()
        {
            try
            {
                var localCertBytes = Convert.FromBase64String(Certificate);
                return string.IsNullOrEmpty(Password)
                      ? new X509Certificate2(localCertBytes, (string)null, X509KeyStorageFlags.MachineKeySet)
                      : new X509Certificate2(localCertBytes, Password, X509KeyStorageFlags.MachineKeySet);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not load the provided certificate.", e);
            }
        }
    }

    public class UnsecuredSetupInfo
    {
        public bool EnableExperimentalFeatures { get; set; }
        public StudioConfiguration.StudioEnvironment Environment { get; set; }
        public List<string> Addresses { get; set; }
        public int Port { get; set; }
        public int TcpPort { get; set; }
        public string LocalNodeTag { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Addresses)] = new DynamicJsonArray(Addresses),
                [nameof(Port)] = Port,
                [nameof(TcpPort)] = TcpPort,
                [nameof(EnableExperimentalFeatures)] = EnableExperimentalFeatures,
                [nameof(Environment)] = Environment,
                [nameof(LocalNodeTag)] = LocalNodeTag
            };
        }
    }

    public class ContinueSetupInfo
    {
        public string NodeTag { get; set; }
        public bool RegisterClientCert { get; set; }
        public string Zip { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(RegisterClientCert)] = RegisterClientCert,
                [nameof(Zip)] = Zip
            };
        }
    }

    public class ListDomainsInfo
    {
        public License License { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License.ToJson(),
            };
        }
    }

    public class ClaimDomainInfo
    {
        public License License { get; set; }
        public string Domain { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License.ToJson(),
                [nameof(Domain)] = Domain
            };
        }
    }

    public class RegistrationInfo
    {
        public License License { get; set; }
        public string Domain { get; set; }
        public string RootDomain { get; set; }
        public string Challenge { get; set; }
        public List<RegistrationNodeInfo> SubDomains { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License.ToJson(),
                [nameof(Domain)] = Domain,
                [nameof(RootDomain)] = RootDomain,
                [nameof(Challenge)] = Challenge,
                [nameof(SubDomains)] = SubDomains.Select(o => o.ToJson()).ToArray()
            };
        }
    }

    public class RegistrationNodeInfo
    {
        public List<string> Ips { get; set; }
        public string SubDomain { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Ips)] = new DynamicJsonArray(Ips),
                [nameof(SubDomain)] = SubDomain,
            };
        }
    }

    public class SubDomainAndIps
    {
        public string SubDomain { get; set; }
        public List<string> Ips { get; set; }

        public SubDomainAndIps()
        {
            Ips = new List<string>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SubDomain)] = SubDomain,
                [nameof(Ips)] = new DynamicJsonArray(Ips)
            };
        }
    }

    public class UserDomainsAndLicenseInfo
    {
        public UserDomainsWithIps UserDomainsWithIps { get; set; }
        public int MaxClusterSize { get; set; }
        public LicenseType LicenseType { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(UserDomainsWithIps)] = UserDomainsWithIps.ToJson(),
                [nameof(MaxClusterSize)] = MaxClusterSize,
                [nameof(LicenseType)] = LicenseType
            };
        }
    }

    public class UserDomainsWithIps
    {
        public string[] Emails { get; set; }
        public string[] RootDomains { get; set; }
        public Dictionary<string, List<SubDomainAndIps>> Domains { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Emails)] = Emails,
                [nameof(RootDomains)] = RootDomains,
                [nameof(Domains)] = DynamicJsonValue.Convert(Domains)
            };
        }
    }

    public class UserDomainsResult
    {
        public string[] Emails { get; set; }
        public string[] RootDomains { get; set; }
        public Dictionary<string, List<string>> Domains { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Emails)] = Emails,
                [nameof(RootDomains)] = RootDomains,
                [nameof(Domains)] = DynamicJsonValue.Convert(Domains)
            };
        }
    }

    public class RegistrationResult
    {
        public string Status { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Status)] = Status
            };
        }
    }

    public enum SetupStage
    {
        Initial = 0,
        Agreement,
        Setup,
        Validation,
        GenerateCertificate,
        Finish
    }

    public class SetupProgressAndResult : IOperationResult, IOperationProgress
    {
        public long Processed { get; set; }
        public long Total { get; set; }
        public string Certificate { get; set; }
        public string Readme { get; set; }
        public readonly ConcurrentQueue<string> Messages;
        public byte[] SettingsZipFile; // not sent as part of the result

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");

        public SetupProgressAndResult()
        {
            Messages = new ConcurrentQueue<string>();
            Certificate = null;
        }

        public string Message { get; private set; }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue(GetType())
            {
                [nameof(Processed)] = Processed,
                [nameof(Total)] = Total,
                [nameof(Readme)] = Readme,
                [nameof(Messages)] = Messages.ToArray()
            };

            if (Certificate != null)
                json[nameof(Certificate)] = Certificate;

            return json;
        }

        public void AddWarning(string message)
        {
            AddMessage("WARNING", message);
        }

        public void AddInfo(string message)
        {
            AddMessage("INFO", message);
        }

        public void AddError(string message, Exception ex = null)
        {
            AddMessage("ERROR", message, ex);
        }

        private void AddMessage(string type, string message, Exception ex = null) //<-- remember last message here
        {
            Message = $"[{SystemTime.UtcNow:T} {type}] {message}";
            Messages.Enqueue(Message);
            if (Logger.IsInfoEnabled)
                Logger.Info(Message, ex);
        }

        public bool ShouldPersist => false;
    }

    public class SetupSettings
    {
        public Node[] Nodes;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Nodes)] = Nodes != null ? new DynamicJsonArray(Nodes.Select(x => x.ToJson())) : null
            };
        }

        public class Node
        {
            public string Tag { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Tag)] = Tag
                };
            }
        }
    }
}
