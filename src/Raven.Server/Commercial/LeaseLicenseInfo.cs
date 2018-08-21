using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide.Operations;

namespace Raven.Server.Commercial
{
    public class LeaseLicenseInfo
    {
        public License License { get; set; }

        public BuildNumber BuildInfo { get; set; }

        public string ClusterId { get; set; }

        public int UtilizedCores { get; set; }

        public string NodeTag { get; set; }

        public StudioConfiguration.StudioEnvironment StudioEnvironment { get; set; }

        public OsInfo OsInfo { get; set; }
    }
}
