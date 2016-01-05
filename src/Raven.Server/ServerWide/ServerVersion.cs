using System.Linq;
using System.Reflection;

namespace Raven.Server.ServerWide
{
    public class ServerVersion
    {
        private static string _buildVersion;
        private static string _commitHash;
        private static string _version;


        private static RavenVersionAttribute GetRavenVersionAttribute()
        {
            return (RavenVersionAttribute)
                typeof (ServerVersion).GetTypeInfo()
                    .Assembly.GetCustomAttributes(typeof (RavenVersionAttribute))
                    .Single();
        }
        public static string Version => _version ?? (_version = GetRavenVersionAttribute().Version);

        public static string Build => _buildVersion ?? (_buildVersion = GetRavenVersionAttribute().Build);


        public static string CommitHash => _commitHash ?? (_commitHash = GetRavenVersionAttribute().CommitHash);

    }
}