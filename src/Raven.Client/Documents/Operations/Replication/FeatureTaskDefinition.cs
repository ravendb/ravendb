using System.Collections.Generic;
using Raven.Client.Exceptions.Security;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public abstract class FeatureTaskDefinition : IDynamicJsonValueConvertible
    {
        public List<string> Certificates; // thumbprint
        public string Name;
        public long TaskId;

        protected FeatureTaskDefinition() { }

        protected FeatureTaskDefinition(string name)
        {
            Name = name;
        }

        public virtual DynamicJsonValue ToJson()
        {
            DynamicJsonArray certs = null;
            if (Certificates != null)
            {
                certs = new DynamicJsonArray(Certificates);
            }
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Certificates)] = certs,
                [nameof(TaskId)] = TaskId
            };
        }

        public void Validate(string thumbprint)
        {
            if (string.IsNullOrEmpty(thumbprint))
            {
                if (Certificates == null || Certificates.Count == 0)
                    return;

                throw new AuthorizationException($"Certificate is needed for pull replication '{Name}'");
            }

            if (Certificates.Contains(thumbprint) == false)
            {
                throw new AuthorizationException($"Certificate with the thumbprint {thumbprint} was not found for pull replication '{Name}'");
            }
        }
    }

}
