using System;

namespace Raven.Client.Documents.Operations.Replication
{
    public class DetailedReplicationHubAccess
    {
        public string Name;
        public string Thumbprint;
        public string Certificate;
        public DateTime NotBefore, NotAfter;
        public string Subject;
        public string Issuer;

        public string[] AllowedHubToSinkPaths;
        public string[] AllowedSinkToHubPaths;

        internal static string[] Preferred(string[] a, string[] b)
        {
            if (a != null && a.Length > 0)
                return a;
            return b ?? Array.Empty<string>();
        }
    }

    internal class ReplicationHubAccessResponse
    {
        public long RaftCommandIndex { get; set; }
    }
}