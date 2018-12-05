using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public abstract class FeatureTaskDefinition : IDynamicJsonValueConvertible
    {
        public Dictionary<string, string> Certificates; // <thumbprint, base64 cert>
        public string Name;
        public long TaskId;
        public bool Disabled;

        protected FeatureTaskDefinition() { }

        protected FeatureTaskDefinition(string name)
        {
            Name = name;
        }

        public virtual DynamicJsonValue ToJson()
        {
            DynamicJsonValue certs = null;
            if (Certificates != null)
            {
                certs = DynamicJsonValue.Convert(Certificates);
            }
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Certificates)] = certs,
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled
            };
        }

        public bool CanAccess(string thumbprint)
        {
            if (string.IsNullOrEmpty(thumbprint))
            {
                return false;
            }

            if (Certificates?.ContainsKey(thumbprint) == true)
                return true; // we will authenticate the certificate later on the tcp level.

            return false;
        }

        public void Validate(bool useSsl)
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));

            if (useSsl == false)
            {
                if (Certificates?.Count > 0)
                {
                    throw new InvalidOperationException("Your server is unsecured and therefore you can't define pull replication with a certificate.");
                }
            }
        }
    }
}
