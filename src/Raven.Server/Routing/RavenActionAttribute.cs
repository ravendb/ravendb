using System;

namespace Raven.Server.Routing
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class RavenActionAttribute : Attribute
    {
        public bool IsDebugInformationEndpoint { get; set; }

        public bool DisableOnCpuCreditsExhaustion { get; set; }

        public CorsMode CorsMode { get; set; }

        public string Path { get; }

        public string Method { get; }

        public AuthorizationStatus RequiredAuthorization { get; set; }

        public bool SkipUsagesCount { get; set; }

        public bool SkipLastRequestTimeUpdate { get; set; }

        public bool IsPosixSpecificEndpoint { get; set; }

        public RavenActionAttribute(string path, string method, AuthorizationStatus requireAuth, bool isDebugInformationEndpoint = false,
            bool isPosixSpecificEndpoint = false, CorsMode corsMode = CorsMode.None)
        {
            Path = path;
            Method = method;
            IsDebugInformationEndpoint = isDebugInformationEndpoint;
            RequiredAuthorization = requireAuth;
            IsPosixSpecificEndpoint = isPosixSpecificEndpoint;
            CorsMode = corsMode;
        }
    }

    public class RavenShardedActionAttribute : Attribute
    {
        public string Path { get; }

        public string Method { get; }

        public RavenShardedActionAttribute(string path, string method)
        {
            Path = path;
            Method = method;
        }
    }

    public enum AuthorizationStatus
    {
        ClusterAdmin,
        Operator,
        DatabaseAdmin,
        ValidUser,
        UnauthenticatedClients,
        RestrictedAccess
    }
}
