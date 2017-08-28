namespace Raven.Client.ServerWide.Tcp
{
    public class TcpConnectionHeaderResponse
    {
        public TcpConnectionStatus Status { get; set; }
        public string Message { get; set; }
    }

    public enum TcpConnectionStatus
    {
        Ok,
        AuthorizationFailed,
        TcpVersionMissmatch
    }
}
