using System.Collections.Generic;

namespace rvn
{
    public class HelmInfo
    {
        public string StorageSize { get; set; } = "10Gi";
        public string RavenImageTag { get; set; } = "latest";
        public string ImagePullPolicy { get; set; } = "IfNotPresent";
        public string IngressClassName { get; set; } = "nginx";

        public List<string> NodeTags { get; set; }
        public string Domain { get; set; }
        public string Email { get; set; }
        public string SetupMode { get; set; }
        public string License { get; set; }
    }
}
