using System.Collections.Generic;

namespace Raven.Server.ServerWide
{
    public class CertificateReplacement
    {
        public string Certificate;
        public string Thumbprint;
        public string OldThumbprint;
        public int Confirmations;
        public int Replaced;
        public bool ReplaceImmediately;
        // Hold the list of the nodes that confirmed/replaced. Nodes working with legacy will have '*' as their tag
        // If there is a legacy node in the cluster, will revert to old behavior - use 'confirmations'/'replaced' count to check if everyone confirmed.
        public HashSet<string> ConfirmedNodes;
        public HashSet<string> ReplacedNodes;

        public const string CertReplaceAlertTitle = "Server Certificate Replacement";
        public const string CertificateReplacementDoc = "server/cert";
    }
}
