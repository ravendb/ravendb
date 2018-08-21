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

        public const string CertReplaceAlertTitle = "Server Certificate Replacement";
        public const string CertificateReplacementDoc = "server/cert";
    }
}
