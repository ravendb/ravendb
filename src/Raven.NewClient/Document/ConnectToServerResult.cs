using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Document
{
    public class ConnectToServerResult
    {
        public NetworkStream Stream { get; set; }
        public string OAuthToken { get; set; }
    }
}
