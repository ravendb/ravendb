namespace Raven.Client.ServerWide.Tcp
{
    public class TcpConnectionHeaderResponse
    {
        public TcpConnectionStatus Status { get; set; }
        public string Message { get; set; }
        public int Version { get; set; }
        public TcpConnectionHeaderMessage.LicensedFeatures LicensedFeatures { get; set; }
    }

    public enum TcpConnectionStatus
    {
        Ok,
        AuthorizationFailed,
        TcpVersionMismatch,
        InvalidNetworkTopology
    }
}
