using System.Collections.Generic;
using System.Text.Json.Serialization;
using Raven.Server.Commercial;

namespace rvn
{
    public class CreateSetupDto
    {
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

        public class Cluster
        {
            [JsonPropertyName("Nodes")]
            public List<Node> Nodes;
        }

        public class Setup
        {
            [JsonPropertyName("License")]
            public License License;

            [JsonPropertyName("RootDomain")]
            public string RootDomain;   
            
            [JsonPropertyName("Domain")]
            public string Domain;   
            
            [JsonPropertyName("Email")]
            public string Email;

            [JsonPropertyName("Cluster")]
            public Cluster Cluster;

            [JsonPropertyName("Password")]
            public string Password;
        }

        public class Root
        {
            [JsonPropertyName("setup")]
            public Setup Setup;
        }
    }
}
