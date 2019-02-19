using System;
using System.IO;
using Raven.Client.ServerWide.Tcp;

namespace Raven.Server.Rachis
{
    public class RachisConnection
    {
        public Stream Stream { get; set; }
        public Action Disconnect { get; set; }
        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; set; }
    }
}
