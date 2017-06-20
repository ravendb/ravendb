using Raven.Client.Http;

namespace Raven.Client.Documents.Session
{
    public class SessionOptions
    {
        public string Database { get; set; }
        public RequestExecutor RequestExecutor { get; set; }
    }
}
