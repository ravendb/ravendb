using System.Collections.Generic;
using System.Text.Json.Serialization;
using Raven.Server.Commercial;

namespace rvn
{
    public class CreateSetupDto
    {
        public class Node
        {
            public string Tag;

            public string Ip;

            public int TcpPort;

            public int HttpPort;

            public string ExternalIp;
        }

        public class Setup
        {
            public License License;

            public string RootDomain;   
            
            public string Domain;   
            
            public string Email;

            public SetupInfo.NodeInfo[] Cluster;
            
            public string Password;
        }

        public class Root
        {
            public Setup Setup;
        }
    }
}
