using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class SetupInfo
    {
        public License License { get; set; }
        public string Domain { get; set; }
        public bool ModifyLocalServer { get; set; }
        public Dictionary<string, NodeInfo> NodeSetupInfos { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License.ToJson(),
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
                    [nameof(Ips)] = Ips.ToArray(),

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

    public class RegistrationResult
    {
        public RegistrationStatus Status { get; set; }
        public string Message { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Status)] = Status.ToString(),
                [nameof(Message)] = Message
            };
        }
    }

    public enum RegistrationStatus
    {
        Pending,
        Done,
        Error
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

        public void AddError(string message)
        {
            AddMessage("ERROR", message);
        }

        private void AddMessage(string type, string message) //<-- remember last message here
        {
            Message = $"[{SystemTime.UtcNow:T} {type}] {message}";
            Messages.Add(Message);
        }

        public bool ShouldPersist => false;
    }
}
