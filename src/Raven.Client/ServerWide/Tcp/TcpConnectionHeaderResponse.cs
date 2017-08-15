namespace Raven.Client.ServerWide.Tcp
{
    public class TcpConnectionHeaderResponse
    {
        public bool AuthorizationSuccessful { get; set; }
        public bool WrongOperationTcpVersion { get; set; }
        public string Message { get; set; }
    }
}
