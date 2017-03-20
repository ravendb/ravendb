namespace Raven.Client.Server.Tcp
{
    public class TcpConnectionHeaderResponse
    {
        public enum AuthorizationStatus
        {
            AuthorizationTokenRequired,
            Forbidden,
            Success,
            BadAuthorizationToken,
            ExpiredAuthorizationToken,
            ForbiddenReadOnly
        }

        public AuthorizationStatus Status { get; set; }

    }
}
