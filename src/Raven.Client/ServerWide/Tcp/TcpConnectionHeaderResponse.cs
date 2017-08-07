namespace Raven.Client.ServerWide.Tcp
{
    public class TcpConnectionHeaderResponse
    {
        public bool AuthorizationSuccessful { get; set; }
        public string Message { get; set; }
    }
}
