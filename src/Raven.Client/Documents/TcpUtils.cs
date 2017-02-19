using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Server.Commands;

namespace Raven.Client.Documents
{
    internal static class TcpUtils
    {
        internal static Stream WrapStreamWithSsl(TcpClient tcpClient, TcpConnectionInfo info)
        {
            Stream stream = tcpClient.GetStream();
            if (info.Certificate != null)
            {
                var expectedCert = new X509Certificate2(Convert.FromBase64String(info.Certificate));
                var sslStream = new SslStream(stream, false, (sender, actualCert, chain, errors) => expectedCert.Equals(actualCert));
                stream = sslStream;
            }
            return stream;
        }
    }
}