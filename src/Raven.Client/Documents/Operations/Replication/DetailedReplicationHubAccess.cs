using System;

namespace Raven.Client.Documents.Operations.Replication
{
    public class DetailedReplicationHubAccess
    {
        public string Name;
        public string Thumbprint;
        public DateTime NotBefore, NotAfter;
        public string Subject;
        public string Issuer;
        
        public string[] AllowedWritePaths;
        public string[] AllowedReadPaths;

        internal static string[] Preferred(string[] a, string[] b)
        {
            if (a != null && a.Length > 0)
                return a;
            return b ?? Array.Empty<string>();
        }

    }
}
