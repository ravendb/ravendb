#if !(NETSTANDARD2_0 || NETSTANDARD2_1 || NETCOREAPP2_1 || NETCOREAPP3_1)
#define TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
#endif

#if !(NETSTANDARD2_0 || NETSTANDARD2_1 || NETCOREAPP2_1)
#define SSL_STREAM_CIPHERSUITESPOLICY_SUPPORT
#endif

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Commands;
using Sparrow.Logging;

namespace Raven.Client.Util
{
    internal static class TcpUtils
    {
        internal const SslProtocols SupportedSslProtocols =
#if NETSTANDARD2_0 || NETSTANDARD2_1 || NETCOREAPP2_1
            SslProtocols.Tls12;
#else
            SslProtocols.Tls13 | SslProtocols.Tls12;

#endif

        private static void SetTimeouts(TcpClient client, TimeSpan timeout)
        {
            client.SendTimeout = (int)timeout.TotalMilliseconds;
            client.ReceiveTimeout = (int)timeout.TotalMilliseconds;
        }

        internal static async Task<(TcpClient Client, string Url)> ConnectSocketAsync(
            TcpConnectionInfo connection,
            TimeSpan timeout,
            Logger log
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
            ,
            CancellationToken token = default
#endif
            )
        {
            try
            {
                return await ConnectAsyncWithPriority(
                    connection,
                    timeout
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
                    ,
                    token
#endif
                    )
                    .ConfigureAwait(false);
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

        public static async Task<TcpClient> ConnectAsync(
            string url,
            TimeSpan? timeout = null,
            bool useIPv6 = false
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
            ,
            CancellationToken token = default
#endif
            )
        {
            var uri = new Uri(url);

            var isIPv6 = uri.HostNameType == UriHostNameType.IPv6;
            var tcpClient = NewTcpClient(timeout, isIPv6);

            try
            {
                if (isIPv6)
                {
                    var ipAddress = IPAddress.Parse(uri.Host);
                    await tcpClient.ConnectAsync(
                        ipAddress,
                        uri.Port
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
                        ,
                        token
#endif
                        )
                        .ConfigureAwait(false);
                }
                else
                {
                    await tcpClient.ConnectAsync(
                        uri.Host,
                        uri.Port
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
                        ,
                        token
#endif
                        )
                        .ConfigureAwait(false);
                }
            }
            catch (NotSupportedException)
            {
                tcpClient.Dispose();

                if (useIPv6)
                    throw;

                return await ConnectAsync(
                    url,
                    timeout,
                    useIPv6: true
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
                    ,
                    token
#endif
                    ).ConfigureAwait(false);
            }
            catch
            {
                tcpClient.Dispose();
                throw;
            }

            return tcpClient;
        }

        internal static async Task<Stream> WrapStreamWithSslAsync(
            TcpClient tcpClient,
            TcpConnectionInfo info,
            X509Certificate2 storeCertificate,
#if SSL_STREAM_CIPHERSUITESPOLICY_SUPPORT
            CipherSuitesPolicy cipherSuitesPolicy,
#endif
            TimeSpan? timeout
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
            ,
            CancellationToken token = default
#endif
            )
        {
            var networkStream = tcpClient.GetStream();
            if (timeout != null)
            {
                networkStream.ReadTimeout =
                    networkStream.WriteTimeout = (int)timeout.Value.TotalMilliseconds;
            }

            if (info.Certificate == null)
                return networkStream;

            var expectedCert = new X509Certificate2(Convert.FromBase64String(info.Certificate), (string)null, X509KeyStorageFlags.MachineKeySet);
            var sslStream = new SslStream(networkStream, false, (sender, actualCert, chain, errors) => expectedCert.Equals(actualCert));

            var targetHost = new Uri(info.Url).Host;
            var clientCertificates = new X509CertificateCollection(new X509Certificate[] { storeCertificate });

#if SSL_STREAM_CIPHERSUITESPOLICY_SUPPORT
            await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = targetHost,
                ClientCertificates = clientCertificates,
                EnabledSslProtocols = SupportedSslProtocols,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                CipherSuitesPolicy = cipherSuitesPolicy
            }
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
                ,
                token
#endif
            ).ConfigureAwait(false);
#else
            await sslStream.AuthenticateAsClientAsync(targetHost, clientCertificates, SupportedSslProtocols, checkCertificateRevocation: false).ConfigureAwait(false);
#endif

            return sslStream;
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
                tcpClient = new TcpClient(AddressFamily.InterNetwork);
            }

            tcpClient.NoDelay = true;
            tcpClient.LingerState = new LingerOption(true, 5);

            if (timeout.HasValue)
                SetTimeouts(tcpClient, timeout.Value);

            Debug.Assert(tcpClient.Client != null);
            return tcpClient;
        }

        internal static async Task<(TcpClient Client, string Url)> ConnectAsyncWithPriority(
            TcpConnectionInfo info,
            TimeSpan? tcpConnectionTimeout
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
            ,
            CancellationToken token = default
#endif
            )
        {
            TcpClient tcpClient;

            if (info.Urls != null)
            {
                foreach (var url in info.Urls)
                {
                    try
                    {
                        tcpClient = await ConnectAsync(
                            url,
                            tcpConnectionTimeout
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
                            ,
                            token: token
#endif
                            ).ConfigureAwait(false);
                        return (tcpClient, url);
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }

            tcpClient = await ConnectAsync(
                info.Url,
                tcpConnectionTimeout
#if TCP_CLIENT_CANCELLATIONTOKEN_SUPPORT
                ,
                token: token
#endif
                )
                .ConfigureAwait(false);

            return (tcpClient, info.Url);
        }
    }
}
