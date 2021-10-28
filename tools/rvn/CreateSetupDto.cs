using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace rvn
{
    public class CreateSetupDto
    {
        public class License
        {
            [JsonPropertyName("Id")]
            public string Id;

            [JsonPropertyName("Name")]
            public string Name;

            [JsonPropertyName("Keys")]
            public List<string> Keys;
        }

        public class Node
        {
            [JsonPropertyName("Tag")]
            public string Tag;

            [JsonPropertyName("Ip")]
            public string Ip;

            [JsonPropertyName("TcpPort")]
            public int TcpPort;

            [JsonPropertyName("HttpPort")]
            public int HttpPort;

            [JsonPropertyName("ExternalIp")]
            public string ExternalIp;
        }

        public class Nodes
        {
            [JsonPropertyName("Node")]
            public Node Node;
        }

        public class Cluster
        {
            [JsonPropertyName("Nodes")]
            public List<Nodes> Nodes;
        }

        public class Setup
        {
            [JsonPropertyName("License")]
            public License License;

            [JsonPropertyName("Domain")]
            public string Domain;

            [JsonPropertyName("Cluster")]
            public Cluster Cluster;
        }

        public class Root
        {
            [JsonPropertyName("setup")]
            public Setup Setup;
        }
    }
}
