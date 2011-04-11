using System;
using System.Linq;
using System.Net.NetworkInformation;
using log4net;
using Raven.Http;

namespace Raven.Database.Util
{
    public static class PortUtil
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof (PortUtil));

        const int DefaultPort = 8080;

        public static int GetPort(string portStr)
        {
            if (portStr == "*" || string.IsNullOrWhiteSpace(portStr))
            {
                var autoPort = FindPort();
                if (autoPort != DefaultPort)
                {
                    logger.InfoFormat("Default port {0} was not available, so using available port {1}", DefaultPort,
                                      autoPort);
                }
                return autoPort;
            }

            int port;
            if (int.TryParse(portStr, out port) == false)
                return DefaultPort;

            return port;
        }

        private static int FindPort()
        {
            var tcpConnInfoArray = IPGlobalProperties
                .GetIPGlobalProperties()
                .GetActiveTcpConnections();

            for (var port = DefaultPort; port < DefaultPort + 1024; port++)
            {
                var portCopy = port;
                if (tcpConnInfoArray.All(tcpi => tcpi.LocalEndPoint.Port != portCopy))
                    return port;
            }

            return DefaultPort;
        }
    }
}