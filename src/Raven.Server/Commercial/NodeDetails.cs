using Raven.Client.ServerWide.Operations;

namespace Raven.Server.Commercial
{
    public class NodeDetails
    {
        public string NodeTag { get; set; }

        public int AssignedCores { get; set; }

        public int NumberOfCores { get; set; }

        public double InstalledMemoryInGb { get; set; }

        public double UsableMemoryInGb { get; set; }

        public BuildNumber BuildInfo { get; set; }
    }
}
