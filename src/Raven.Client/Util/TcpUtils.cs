using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Commands;
using Sparrow.Logging;

namespace Raven.Client.Util
{
    internal static class TcpUtils
    {
        private static void SetTimeouts(TcpClient client, TimeSpan timeout)
        {
            client.SendTimeout = (int)timeout.TotalMilliseconds;
            client.ReceiveTimeout = (int)timeout.TotalMilliseconds;
        }

        internal static async Task<TcpClient> ConnectSocketAsync(TcpConnectionInfo connection, TimeSpan timeout, Logger log)
        {
            try
            {
                return await ConnectAsync(connection.Url, timeout).ConfigureAwait(false);
            }
            catch (AggregateException ae) when (ae.InnerException is SocketException)
            {
                if (log.IsInfoEnabled)
                    log.Info(
                        $"Failed to connect to remote replication destination {connection.Url}. Socket Error Code = {((SocketException)ae.InnerException).SocketErrorCode}",
                        ae.InnerException);
                throw;
            }
            catch (AggregateException ae) when (ae.InnerException is OperationCanceledException)
            {
                if (log.IsInfoEnabled)
                    log.Info(
                        $@"Tried to connect to remote replication destination {connection.Url}, but the operation was aborted. 
                            This is not necessarily an issue, it might be that replication destination document has changed at 
                            the same time we tried to connect. We will try to reconnect later.",
                        ae.InnerException);
                throw;
            }
            catch (OperationCanceledException e)
            {
                if (log.IsInfoEnabled)
                    log.Info(
                        $@"Tried to connect to remote replication destination {connection.Url}, but the operation was aborted. 
                            This is not necessarily an issue, it might be that replication destination document has changed at 
                            the same time we tried to connect. We will try to reconnect later.",
                        e);
                throw;
            }
            catch (Exception e)
            {
                if (log.IsInfoEnabled)
                    log.Info($"Failed to connect to remote replication destination {connection.Url}", e);
                throw;
            }
        }

        public static async Task<TcpClient> ConnectAsync(string url, TimeSpan? timeout = null, bool useIPv6 = false)
        {
            var uri = new Uri(url);

            var isIPv6 = uri.HostNameType == UriHostNameType.IPv6;
            var tcpClient = NewTcpClient(timeout, isIPv6);

            try
            {
                if (isIPv6)
                {
                    var ipAddress = IPAddress.Parse(uri.Host);
                    await tcpClient.ConnectAsync(ipAddress, uri.Port).ConfigureAwait(false);
                }
                else
                {
                    await tcpClient.ConnectAsync(uri.Host, uri.Port).ConfigureAwait(false);
                }
            }
            catch (NotSupportedException)
            {
                tcpClient.Dispose();

                if (useIPv6)
                    throw;

                return await ConnectAsync(url, timeout, true).ConfigureAwait(false);
            }
            catch
            {
                tcpClient.Dispose();
                throw;
            }

            return tcpClient;
        }

        internal static async Task<Stream> WrapStreamWithSslAsync(TcpClient tcpClient, TcpConnectionInfo info, X509Certificate2 storeCertificate, 
            TimeSpan? timeout)
        {
            var networkStream = tcpClient.GetStream();
            if (timeout != null)
            {
                networkStream.ReadTimeout =  
                    networkStream.WriteTimeout = (int)timeout.Value.TotalMilliseconds;
            }
            Stream stream = networkStream;
            if (info.Certificate == null)
                return stream;

            var expectedCert = new X509Certificate2(Convert.FromBase64String(info.Certificate), (string)null, X509KeyStorageFlags.MachineKeySet);
            var sslStream = new SslStream(stream, false, (sender, actualCert, chain, errors) => expectedCert.Equals(actualCert));
            await sslStream.AuthenticateAsClientAsync(new Uri(info.Url).Host, new X509CertificateCollection(new X509Certificate[]{storeCertificate}), SslProtocols.Tls12, false).ConfigureAwait(false);
            stream = sslStream;
            return stream;
        }

        private static TcpClient NewTcpClient(TimeSpan? timeout, bool useIPv6)
        {
            // We start with a IPv4 TcpClient and we fallback to use IPv6 TcpClient only if we fail.
            // This is because that dual mode of IPv6 has a timeout of 1 second 
            // which is bigger than the election time in the cluster which is 300ms.
            TcpClient tcpClient;
            if (useIPv6)
            {
                tcpClient = new TcpClient(AddressFamily.InterNetworkV6);
                tcpClient.Client.DualMode = true;
            }
            else
            {
                tcpClient = new TcpClient();
            }
            
            tcpClient.NoDelay = true;            
            tcpClient.LingerState = new LingerOption(true, 5);

            if (timeout.HasValue)
                SetTimeouts(tcpClient, timeout.Value);

            Debug.Assert(tcpClient.Client != null);
            return tcpClient;
        }
    }
}
