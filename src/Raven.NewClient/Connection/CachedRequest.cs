using System;
using System.Collections.Specialized;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Connection
{
    public class CachedRequest
    {
        public RavenJToken Data { get; set; }
        public DateTimeOffset Time;
        public NameValueCollection Headers;
        public string Database;
        public int ReadTime;
        public bool ForceServerCheck;
    }

    public class CachedRequestOp
    {
        public CachedRequest CachedRequest;
        public bool SkipServerCheck;
    }
}
