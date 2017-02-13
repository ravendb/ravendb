using Raven.Client.Properties;

namespace Raven.Server.ServerWide
{
    public class ServerVersion
    {
        private static int? _buildVersion;
        private static string _commitHash;
        private static string _version;
        private static string _fullVersion;
        private static string _semVersion;


        public static string Version => 
            _version ?? (_version = RavenVersionAttribute.Instance.Version);

        public static int Build =>  
            _buildVersion ?? (_buildVersion = RavenVersionAttribute.Instance.BuildVersion).Value;
        public static string CommitHash => 
            _commitHash ?? (_commitHash = RavenVersionAttribute.Instance.CommitHash);
        public static string FullVersion => 
            _fullVersion ?? (_fullVersion = RavenVersionAttribute.Instance.FullVersion);
        public static string SemVersion =>
            _semVersion ?? (_semVersion = Version + ".0." + Build);


        public const int DevBuildNumber = 40;

    }
}