using System.IO;
using System.Net.Sockets;
using Raven.Client.ServerWide.Tcp;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ClusterMaintenanceConnection
    {
        public Stream Stream { get; set; }
        public TcpClient TcpClient { get; set; }
        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; set; }
    }
}
