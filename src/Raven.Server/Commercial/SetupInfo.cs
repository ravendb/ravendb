using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Commercial
{
    public class SetupInfo
    {
        public License License { get; set; }
        public string Email { get; set; }
        public string Domain { get; set; }
        public bool ModifyLocalServer { get; set; }
        public Dictionary<string, NodeInfo> NodeSetupInfos { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License.ToJson(),
                [nameof(Email)] = Email,
                [nameof(Domain)] = Domain,
                [nameof(ModifyLocalServer)] = ModifyLocalServer,
                [nameof(NodeSetupInfos)] = DynamicJsonValue.Convert(NodeSetupInfos)
            };
        }

        public class NodeInfo
        {
            public string Certificate { get; set; }
            public string Password { get; set; }
            public int Port { get; set; }
            public List<string> Ips { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Certificate)] = Certificate,
                    [nameof(Password)] = Password,
                    [nameof(Port)] = Port,
                    [nameof(Ips)] = Ips.ToArray()
                };
            }
        }
    }

    public class UnsecuredSetupInfo
    {
        public string ServerUrl { get; set; }
        public string PublicServerUrl { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ServerUrl)] = ServerUrl,
                [nameof(PublicServerUrl)] = PublicServerUrl
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
        public List<RegistrationNodeInfo> SubDomains { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License.ToJson(),
                [nameof(Domain)] = Domain,
                [nameof(SubDomains)] = SubDomains.Select(o => o.ToJson()).ToArray()
            };
        }
    }

    public class RegistrationNodeInfo
    {
        public List<string> Ips { get; set; }
        public string SubDomain { get; set; }
        public string Challenge { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Ips)] = Ips.ToArray(),
                [nameof(SubDomain)] = SubDomain,
                [nameof(Challenge)] = Challenge
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

    public enum SetupMode
    {
        None,
        Initial,
        LetsEncrypt,
        Secured,
        Unsecured
    }

    public enum SetupStage
    {
        Initial = 0,
        Agreement,
        Setup,
        Validation,
        GenarateCertificate,
        Finish
    }

    public class SetupProgressAndResult : IOperationResult, IOperationProgress
    {
        public long Processed { get; set; }
        public long Total { get; set; }
        public readonly List<string> Messages;
        public byte[] SettingsZipFile;

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("Server");
        
        public SetupProgressAndResult()
        {
            Messages = new List<string>(); // <-- fix this to be thread safe
        }

        public string Message { get; private set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Processed)] = Processed,
                [nameof(Total)] = Total,
                [nameof(Messages)] = Messages
        };
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
            Messages.Add(Message);
            if (Logger.IsInfoEnabled)
                Logger.Info(Message, ex);
        }

        public bool ShouldPersist => false;
    }
}
