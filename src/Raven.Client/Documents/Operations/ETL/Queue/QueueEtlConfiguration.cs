using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue
{
    public class QueueEtlConfiguration : EtlConfiguration<QueueConnectionString>
    {
        private string _destination;
        
        public QueueEtlConfiguration()
        {
            EtlQueues = new List<EtlQueue>();
        }

        public List<EtlQueue> EtlQueues { get; set; }

        public QueueProvider Provider { get; set; }

        public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
        {
            var validationResult = base.Validate(out errors, validateName, validateConnection);
            if (Connection != null && Provider != Connection.Provider)
            {
                errors.Add("Provider must be the same in the ETL configuration and in Connection string.");
                return false;
            }
            return validationResult;
        }
        
        public override string GetDestination()
        {
            var url = Connection.Url;
            return _destination ??= $"@{string.Join(",",url)}";
        }

        public override EtlType EtlType => EtlType.Queue;
        
        public override bool UsingEncryptedCommunicationChannel()
        {            
            //return !Connection.Url.StartsWith("http:", StringComparison.OrdinalIgnoreCase);
            return true;
        }

        public override string GetDefaultTaskName()
        {
            return $"Queue ETL to {ConnectionStringName}";
        }
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(EtlQueues)] = new DynamicJsonArray(EtlQueues.Select(x => x.ToJson()));

            return json;
        }
    }
    
    public class EtlQueue
    {
        public string Name { get; set; }

        public string Type { get; set; }

        public string Source { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Type)] = Type,
                [nameof(Source)] = Source,
            };
        }
    }
}
