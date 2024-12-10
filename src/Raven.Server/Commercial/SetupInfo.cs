using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Raven.Server.Commercial.SetupWizard;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class NodeInfo
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

    public sealed class SetupInfo : SetupInfoBase
    {
        public bool RegisterClientCert { get; set; }
        public DateTime? ClientCertNotAfter { get; set; }
        public License License { get; set; }
        public string Email { get; set; }
        public string Domain { get; set; }
        public string RootDomain { get; set; }
        public string Certificate { get; set; }
        public string Password { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(License)] = License.ToJson();
            json[nameof(Email)] = Email;
            json[nameof(Domain)] = Domain;
            json[nameof(RootDomain)] = RootDomain;
            json[nameof(ZipOnly)] = ZipOnly;
            json[nameof(RegisterClientCert)] = RegisterClientCert;
            json[nameof(ClientCertNotAfter)] = ClientCertNotAfter;
            json[nameof(Certificate)] = Certificate;
            json[nameof(Password)] = Password;
            return json;
        }

        public override void ValidateInfo(CreateSetupPackageParameters parameters)
        {
            var exceptions = new List<Exception>();

            if (License == null)
            {
                exceptions.Add(new InvalidOperationException($"{nameof(License)} must be set"));
            }

            if (License?.Keys is null || License.Keys.Any() == false)
            {
                exceptions.Add(new InvalidOperationException($"{nameof(License.Keys)} must be set"));
            }

            if (string.IsNullOrEmpty(License?.Id.ToString()))
            {
                exceptions.Add(new InvalidOperationException($"{nameof(License.Id)} must be set"));
            }

            if (string.IsNullOrEmpty(License?.Name))
            {
                exceptions.Add(new InvalidOperationException($"{nameof(License.Name)} must be set"));
            }

            if (string.IsNullOrEmpty(Email))
            {
                exceptions.Add(new InvalidOperationException($"{nameof(Email)} must be set"));
            }

            if (string.IsNullOrEmpty(Domain))
            {
                exceptions.Add(new InvalidOperationException($"{nameof(Domain)} must be set"));
            }

            if (string.IsNullOrEmpty(RootDomain))
            {
                exceptions.Add(new InvalidOperationException($"{nameof(RootDomain)} must be set"));
            }
        
            parameters.PackageOutputPath ??= Domain;
            
            if (string.IsNullOrEmpty(parameters.CertPassword) == false)
            {
                Password = parameters.CertPassword;
            }
            
            if (Path.HasExtension(parameters.PackageOutputPath) == false)
            {
                parameters.PackageOutputPath += $"_{DateTime.UtcNow:yyyy-MM-dd HH-mm}.zip";
            }
            else if (Path.GetExtension(parameters.PackageOutputPath)?.Equals(".zip", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException("-o|--package-output-path file name must end with an extension of .zip");
            }
            
            parameters.PackageOutputPath = Path.ChangeExtension(parameters.PackageOutputPath, Path.GetExtension(parameters.PackageOutputPath)?.ToLower());
        }

        public override async Task<byte[]> GenerateZipFile(CreateSetupPackageParameters parameters)
        {
            switch (parameters.Mode)
            {
                case "own-certificate":
                {
                    var certBytes = await File.ReadAllBytesAsync(parameters.CertificatePath, parameters.CancellationToken);
                    var certBase64 = Convert.ToBase64String(certBytes);
                    Certificate = certBase64;
                    return await OwnCertificateSetupUtils.Setup(parameters.SetupInfo, parameters.Progress, parameters.CancellationToken);
                }
                case "lets-encrypt":
                {
                    return await LetsEncryptSetupUtils.Setup(parameters.SetupInfo, parameters.Progress, parameters.RegisterTcpDnsRecords, parameters.AcmeUrl, parameters.CancellationToken);
                }
                default: throw new InvalidOperationException("Invalid mode provided.");
            }
        }

        public X509Certificate2 GetX509Certificate()
        {
            try
            {
                var localCertBytes = Convert.FromBase64String(Certificate);
                return CertificateLoaderUtil.CreateCertificateFromPfx(localCertBytes, Password);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not load the provided certificate.", e);
            }
        }
    }

    public sealed class UnsecuredSetupInfo : SetupInfoBase
    {
        public override void ValidateInfo(CreateSetupPackageParameters parameters)
        {
            List<Exception> exceptions = new ();
            if (NodeSetupInfos is null || NodeSetupInfos.Any() == false)
            {
                exceptions.Add(new InvalidOperationException($"{nameof(NodeSetupInfos)} must be set"));
            }

            foreach (var tag in NodeSetupInfos.Keys.Where(tag => SetupWizardUtils.IsValidNodeTag(tag) == false))
            {
                exceptions.Add(new InvalidOperationException($"Node tags must contain only capital letters.Maximum length should be up to 4 characters/nNode tag - {tag}"));
            }

            foreach (var nodeInfoNode in NodeSetupInfos.Values)
            {
                if (nodeInfoNode?.Addresses is null)
                {
                    exceptions.Add(new InvalidOperationException($"Addresses must be set inside {nameof(NodeSetupInfos)}"));
                }

                if (nodeInfoNode?.Port == 0)
                {
                    nodeInfoNode.Port = Constants.Network.DefaultSecuredRavenDbHttpPort;
                }

                if (nodeInfoNode.TcpPort == 0)
                {
                    nodeInfoNode.TcpPort = Constants.Network.DefaultSecuredRavenDbTcpPort;
                }
            }

            if (string.IsNullOrEmpty(parameters.PackageOutputPath))
            {
                var fileName = $"Unsecure.Cluster.Settings.{DateTime.UtcNow:yyyy-MM-dd HH-mm}.zip ";
                parameters.PackageOutputPath = fileName;
            }
            else if (Path.HasExtension(parameters.PackageOutputPath) == false)
            {
                parameters.PackageOutputPath += $".{DateTime.UtcNow:yyyy-MM-dd HH-mm}.zip";
            }
            else if (Path.GetExtension(parameters.PackageOutputPath)?.Equals(".zip", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException("-o|--package-output-path file name must end with an extension of .zip");
            }
            
            parameters.PackageOutputPath = Path.ChangeExtension(parameters.PackageOutputPath, Path.GetExtension(parameters.PackageOutputPath)?.ToLower());
            
            if (exceptions.Count > 0)
                throw new AggregateException($"{nameof(UnsecuredSetupInfo)} validation exceptions list: ", exceptions);

        }

        public override async Task<byte[]> GenerateZipFile(CreateSetupPackageParameters parameters)
        {
            return await UnsecuredSetupUtils.Setup(parameters.UnsecuredSetupInfo, parameters.Progress, parameters.CancellationToken);
        }

    }

    public sealed class ContinueSetupInfo
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

    public sealed class ClaimDomainInfo
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

    public sealed class RegistrationInfo
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

    public sealed class RegistrationNodeInfo
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

    public sealed class SubDomainAndIps
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

    public sealed class UserDomainsAndLicenseInfo
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

    public sealed class UserDomainsWithIps
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

    public sealed class UserDomainsResult
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

    public sealed class RegistrationResult
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

    public sealed class SetupProgressAndResult : IOperationResult, IOperationProgress
    {
        private readonly Action<(string Message, Exception Exception)> _onMessage;

        public long Processed { get; set; }
        public long Total { get; set; }
        public string Certificate { get; set; }
        public string Readme { get; set; }
        public readonly ConcurrentQueue<string> Messages;
        public byte[] SettingsZipFile; // not sent as part of the result

        public SetupProgressAndResult(Action<(string Message, Exception Exception)> onMessage)
        {
            _onMessage = onMessage;
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

        IOperationProgress IOperationProgress.Clone()
        {
            throw new NotImplementedException();
        }

        bool IOperationProgress.CanMerge => false;

        void IOperationResult.MergeWith(IOperationResult result)
        {
            throw new NotImplementedException();
        }

        bool IOperationResult.CanMerge => false;

        void IOperationProgress.MergeWith(IOperationProgress progress)
        {
            throw new NotImplementedException();
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
            _onMessage.Invoke((Message, ex));
        }

        public bool ShouldPersist => false;
    }

    public sealed class SetupSettings
    {
        public Node[] Nodes;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Nodes)] = Nodes != null
                    ? new DynamicJsonArray(Nodes.Select(x => x.ToJson()))
                    : null
            };
        }

        public sealed class Node
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
