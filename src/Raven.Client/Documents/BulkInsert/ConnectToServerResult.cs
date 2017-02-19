using System.IO;

namespace Raven.Client.Documents.BulkInsert
{
    internal class ConnectToServerResult
    {
        public Stream Stream { get; set; }
        public string OAuthToken { get; set; }
    }
}
