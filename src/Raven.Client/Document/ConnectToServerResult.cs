using System.Net.Sockets;

namespace Raven.Client.Document
{
    public class ConnectToServerResult
    {
        public NetworkStream Stream { get; set; }
        public string OAuthToken { get; set; }
    }
}
