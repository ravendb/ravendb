using System.Linq;
using System.Reflection;

namespace Raven.Server.ServerWide
{
    public class ServerVersion
    {
        private static int? _buildVersion;
        private static string _commitHash;
        private static string _version;
        private static string _fullVersion;

        public static string Version => 
            _version ?? (_version = Client.RavenVersionAttribute.Instance.Version);

        public static int Build =>  
            _buildVersion ?? (_buildVersion = Client.RavenVersionAttribute.Instance.BuildVersion).Value;
        public static string CommitHash => 
            _commitHash ?? (_commitHash = Client.RavenVersionAttribute.Instance.CommitHash);
        public static string FullVersion => 
            _fullVersion ?? (_fullVersion = Client.RavenVersionAttribute.Instance.FullVersion);

    }
}