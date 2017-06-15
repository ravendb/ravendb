using System;
using JetBrains.Annotations;

namespace Raven.Server.Routing
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    [MeansImplicitUse]
    public class RavenActionAttribute : Attribute
    {
        public bool IsDebugInformationEndpoint { get; set; }

        public string Path { get; }

        public string Method { get; }

        public string Description { get; }

        public bool NoAuthorizationRequired { get; set; } // "NeverSecret"

        public bool SkipUsagesCount { get; set; }

        public RavenActionAttribute(string path, string method, string description = null, bool isDebugInformationEndpoint = false)
        {
            Path = path;
            Method = method;
            Description = description;
            IsDebugInformationEndpoint = isDebugInformationEndpoint;
        }
    }
}