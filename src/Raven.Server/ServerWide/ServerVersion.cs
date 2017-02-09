namespace Raven.Server.ServerWide
{
    public class ServerVersion
    {
        private static int? _buildVersion;
        private static string _commitHash;
        private static string _version;
        private static string _fullVersion;

        public static string Version => 
            _version ?? (_version = NewClient.RavenVersionAttribute.Instance.Version);

        public static int Build =>  
            _buildVersion ?? (_buildVersion = NewClient.RavenVersionAttribute.Instance.BuildVersion).Value;
        public static string CommitHash => 
            _commitHash ?? (_commitHash = NewClient.RavenVersionAttribute.Instance.CommitHash);
        public static string FullVersion => 
            _fullVersion ?? (_fullVersion = NewClient.RavenVersionAttribute.Instance.FullVersion);

        public const int DevBuildNumber = 40;

    }
}