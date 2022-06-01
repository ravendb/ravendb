using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue
{
    public class QueueEtlConfiguration : EtlConfiguration<QueueConnectionString>
    {
        public QueueEtlConfiguration()
        {
            Queues = new List<EtlQueue>();
        }

        public List<EtlQueue> Queues { get; set; }

        public QueueBroker BrokerType { get; set; }

        public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
        {
            var validationResult = base.Validate(out errors, validateName, validateConnection);
            if (Connection != null && BrokerType != Connection.BrokerType)
            {
                errors.Add("Broker type must be the same in the ETL configuration and in Connection string.");
                return false;
            }
            return validationResult;
        }
        
        public override string GetDestination()
        {
            return Connection.GetUrl();
        }

        public override EtlType EtlType => EtlType.Queue;
        
        public override bool UsingEncryptedCommunicationChannel()
        {            
            //return !Connection.Url.StartsWith("http:", StringComparison.OrdinalIgnoreCase);
            //todo: check with arek what should we do with this
            return true;
        }

        public override string GetDefaultTaskName()
        {
            return $"Queue ETL to {ConnectionStringName}";
        }
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Queues)] = new DynamicJsonArray(Queues.Select(x => x.ToJson()));

            return json;
        }
    }
    
    public class EtlQueue
    {
        public string Name { get; set; }

        public bool DeleteProcessedDocuments { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(DeleteProcessedDocuments)] = DeleteProcessedDocuments
            };
        }
    }
}
