using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Server.Commands;

namespace Raven.Client.Document
{
    public static class TcpUtils
    {
        public static async Task<Stream> WrapStreamWithSsl(TcpClient tcpClient, TcpConnectionInfo info)
        {
            Stream stream = tcpClient.GetStream();
            if (info.Certificate != null)
            {
                var Cert = new X509Certificate2(Convert.FromBase64String(info.Certificate));
                var sslStream = new SslStream(stream, false, (sender, certificate, chain, errors) => Cert.Equals(certificate));
                await
                    sslStream.AuthenticateAsClientAsync("RavenDB", new X509Certificate2Collection { Cert }, SslProtocols.Tls12,
                        false);
                stream = sslStream;
            }
            return stream;
        }
    }
}