using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;
using Rachis;
using Voron.Impl.FileHeaders;

namespace RachisTests
{
    public static class TcpConnector
    {
        public static Dictionary<string,RaftEngine> IdsToRaftEngines { get; set; }
        private static short startingPort = 9876;
        private static short maxPort = 9999;
        private static Dictionary<string, List<Connection>> _connections = new Dictionary<string, List<Connection>>();
        private class Connection: IEqualityComparer<Connection>, IDisposable
        {
            public Connection(string source, string destination, TcpClient client, TcpListener server, TcpClient serverClient)
            {
                Source = source;
                Destination = destination;
                Client = client;
                Server = server;
                ServerClient = serverClient;

            }

            public TcpClient Client { get; }
            public TcpClient ServerClient { get; }
            public TcpListener Server { get; }
            public string Source { get;}
            public string Destination { get;}

            public bool Equals(Connection x, Connection y)
            {
                return x.Destination == y.Destination && x.Source == y.Source;
            }

            public int GetHashCode(Connection connection)
            {
                return (23*31 + connection.Source.GetHashCode())*31 + connection.Destination.GetHashCode();
            }

            public void Dispose()
            {
                Client?.GetStream()?.Dispose();
                Client?.Dispose();
                ServerClient?.GetStream()?.Dispose();
                ServerClient?.Dispose();
                Server?.Server?.Dispose();
            }
        }

        public static Stream Connect(string source, string destination)
        {
            List<Connection> connections;
            if (_connections.TryGetValue(source, out connections))
            {
                var res = connections.FirstOrDefault(x => x.Destination == destination);
                if (res != null)
                {
                    connections.Remove(res);
                    res.Dispose();
                }
            }

            RaftEngine engine = null;     
            if(IdsToRaftEngines?.TryGetValue(destination,out engine) == false)
                throw new InvalidOperationException($"Can't connect to un-configured destination:{destination}");
            bool connected = false;
            TcpListener listener = null;
            short port;
            //create the server
            do
            {
                port = startingPort++;
                listener = new TcpListener(IPAddress.Loopback, port);
                listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                try
                {
                    listener.Start();
                }
                catch (Exception ex)
                {
                    if (startingPort > maxPort)
                        throw new IOException("Unable to start tcp listener on loopback on all ports", ex);
                    continue;
                }
                connected = true;
            } while (connected == false);

            var tcpClient = new TcpClient();
            tcpClient.NoDelay = true;
            tcpClient.Client.Connect(IPAddress.Loopback, port);
            
            var stream = tcpClient.GetStream();
            //stream.Write(connectString,0, connectString.Length);
            var serverClient = listener.AcceptTcpClientAsync().Result;
            serverClient.NoDelay = true;
            //var read = serverClient.GetStream().Read(connectString, 0, connectString.Length);
            var connection = new Connection(source, destination, tcpClient, listener,serverClient);
            
            if (_connections.TryGetValue(source, out connections) == false)
            {
                connections = new List<Connection> { connection };
                _connections[source] = connections;
            }
            else connections.Add(connection);
            Task.Factory.StartNew(()=> { engine.HandleNewConnection(serverClient.GetStream()); });
            return tcpClient.GetStream();
        }

        public static void Dispose()
        {
            foreach (var connectionList in _connections.Values)
            {
                foreach (var connection in connectionList)
                {
                    connection.Dispose();
                }
            }
        }

    }
}
