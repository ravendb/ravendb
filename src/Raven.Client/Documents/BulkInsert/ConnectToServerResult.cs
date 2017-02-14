using System.Net.Sockets;

namespace Raven.Client.Documents.BulkInsert
{
    internal class ConnectToServerResult
    {
        public NetworkStream Stream { get; set; }
        public string OAuthToken { get; set; }
    }
}
