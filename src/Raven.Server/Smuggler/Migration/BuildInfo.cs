using System.Collections.Generic;

namespace Raven.Server.Smuggler.Migration
{
    public class BuildInfo
    {
        public int BuildVersion { get; set; }

        public string ProductVersion { get; set; }

        public MajorVersion MajorVersion { get; set; }

        public string FullVersion { get; set; }
    }

    public class BuildInfoWithDatabaseNames : BuildInfo
    {
        public List<string> DatabaseNames { get; set; }
    }
}
