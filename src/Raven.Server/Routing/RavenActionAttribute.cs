using System;
using System.Net.Http;

namespace Raven.Server.Routing
{
    public class RouteAttribute : Attribute
    {
        public string Path { get; }

        public string Method { get; }

        public RouteAttribute(string path, string method)
        {
            Path = path;
            Method = method;
        }
    }
}