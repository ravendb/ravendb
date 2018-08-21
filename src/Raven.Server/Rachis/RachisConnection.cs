using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
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
