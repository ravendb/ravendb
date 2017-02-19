using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Server.Kestrel.Internal.Http;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Assert = Xunit.Assert;
using TcpListener = System.Net.Sockets.TcpListener;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var basicCluster = new BasicCluster())
            {
                basicCluster.ClusterWithTwoNodes();
            }
        }

       
    }


}