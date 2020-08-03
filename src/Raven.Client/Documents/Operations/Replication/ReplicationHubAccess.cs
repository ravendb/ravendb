using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class ReplicationHubAccess : IDynamicJson
    {
        public string Name;
        public string CertificateBase64;
        
        public string[] Incoming;
        public string[] Outgoing;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Incoming)] = Incoming,
                [nameof(Outgoing)] = Outgoing,
                [nameof(CertificateBase64)] = CertificateBase64,
                
            };
        }

        internal void Validate()
        {
            if (string.IsNullOrEmpty(Name))
                throw new ArgumentNullException(nameof(Name));

            if((Incoming?.Length ?? 0) == 0 && (Outgoing?.Length == 0))
                throw new InvalidOperationException($"Either {nameof(Outgoing)} or {nameof(Incoming)} must have a value, but both were null or empty");

            ValidateAllowedPaths(Incoming);
            ValidateAllowedPaths(Outgoing);
        }
        
        
        private void ValidateAllowedPaths(string[] allowedPaths)
        {
            if ((allowedPaths?.Length ?? 0) == 0)
                return;

            foreach (string path in allowedPaths)
            {
                if (string.IsNullOrEmpty(path))
                    throw new InvalidOperationException("Filtered replication AllowedPaths cannot have an empty / null filter");

                if (path[path.Length - 1] != '*')
                    continue;

                if (path.Length > 1 && path[path.Length - 2] != '/' && path[path.Length - 2] != '-')
                    throw new InvalidOperationException(
                        $"When using '*' at the end of the allowed path, the previous character must be '/' or '-', but got: {path} for {Name}");
            }
        }
    }
}
