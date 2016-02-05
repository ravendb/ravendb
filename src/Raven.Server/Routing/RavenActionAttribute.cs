using System;
using System.Net.Http;

namespace Raven.Server.Routing
{
    public class RavenActionAttribute : Attribute
    {
        public string Path { get; }

        public string Method { get; }

        public RavenActionAttribute(string path, string method)
        {
            Path = path;
            Method = method;
        }
    }
}