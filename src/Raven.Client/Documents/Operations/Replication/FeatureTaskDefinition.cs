using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public abstract class FeatureTaskDefinition : IDynamicJsonValueConvertible
    {
        public List<string> Certificates; // base64
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
                foreach (var certificate in Certificates)
                {
                    var x509Certificate2 = new X509Certificate2(Convert.FromBase64String(certificate));
                }
                certs = new DynamicJsonArray(Certificates);
            }
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Certificates)] = certs,
                [nameof(TaskId)] = TaskId
            };
        }

        public bool CanAccess(string thumbprint, out string err)
        {
            err = null;
            if (string.IsNullOrEmpty(thumbprint))
            {
                if (Certificates == null || Certificates.Count == 0)
                    return true;

                err = $"Certificate is needed for pull replication '{Name}'";
                return false;
            }

            foreach (var certificate in Certificates)
            {
                var cert = new X509Certificate2(Convert.FromBase64String(certificate));
                if (cert.Thumbprint == thumbprint)
                {
                    return true;
                }
            }

            err = $"Certificate with the thumbprint {thumbprint} was not found for pull replication '{Name}'";
            return false;
        }

        public void Validate()
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));
        }
    }
}
