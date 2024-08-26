using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Util
{
    internal static class TcpUtils
    {
        internal const SslProtocols SupportedSslProtocols =
#if NETSTANDARD
            SslProtocols.Tls12;
#else
            SslProtocols.Tls13 | SslProtocols.Tls12;

#endif

        private static void SetTimeouts(TcpClient client, TimeSpan timeout)
        {
            client.SendTimeout = (int)timeout.TotalMilliseconds;
            client.ReceiveTimeout = (int)timeout.TotalMilliseconds;
        }

        internal static async Task<ConnectSecuredTcpSocketResult> ConnectSecuredTcpSocketAsReplication(
            TcpConnectionInfo connection,
            X509Certificate2 certificate,
#if !NETSTANDARD
            CipherSuitesPolicy cipherSuitesPolicy,
#endif
            NegotiationCallback negotiationCallback,
            TimeSpan timeout,
            RavenLogger log
#if !NETSTANDARD
            ,
            CancellationToken token = default
#endif
            )
        {
            try
            {
                return await ConnectSecuredTcpSocket(
                    connection,
                    certificate,
#if !NETSTANDARD
                    cipherSuitesPolicy,
#endif
                    TcpConnectionHeaderMessage.OperationTypes.Replication,
                    negotiationCallback,
                    null,
                    timeout,
                    null
#if !NETSTANDARD
                    ,
                    token
#endif
                ).ConfigureAwait(false);
            }
            catch (AggregateException ae) when (ae.InnerException is SocketException)
            {
                if (log.IsDebugEnabled)
                    log.Debug(
                        $"Failed to connect to remote replication destination {connection.Url}. Socket Error Code = {((SocketException)ae.InnerException).SocketErrorCode}",
                        ae.InnerException);
                throw;
            }
            catch (AggregateException ae) when (ae.InnerException is OperationCanceledException)
            {
                if (log.IsDebugEnabled)
                    log.Debug(
                        $@"Tried to connect to remote replication destination {connection.Url}, but the operation was aborted.
                            This is not necessarily an issue, it might be that replication destination document has changed at
                            the same time we tried to connect. We will try to reconnect later.",
                        ae.InnerException);
                throw;
            }
            catch (OperationCanceledException e)
            {
                if (log.IsDebugEnabled)
                    log.Debug(
                        $@"Tried to connect to remote replication destination {connection.Url}, but the operation was aborted.
                            This is not necessarily an issue, it might be that replication destination document has changed at
                            the same time we tried to connect. We will try to reconnect later.",
                        e);
                throw;
            }
            catch (Exception e)
            {
                if (log.IsWarnEnabled)
                    log.Warn($"Failed to connect to remote replication destination {connection.Url}", e);
                throw;
            }
        }

        public static async Task<TcpClient> ConnectAsync(
            string url,
            TimeSpan? timeout = null,
            bool useIPv6 = false
#if !NETSTANDARD
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
#if !NETSTANDARD
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
#if !NETSTANDARD
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
#if !NETSTANDARD
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
#if !NETSTANDARD
            CipherSuitesPolicy cipherSuitesPolicy,
#endif
            TimeSpan? timeout
#if !NETSTANDARD
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

            var expectedCert = CertificateLoaderUtil.CreateCertificate(Convert.FromBase64String(info.Certificate));
            var sslStream = new SslStream(networkStream, false, (sender, actualCert, chain, errors) => expectedCert.Equals(actualCert));

            var targetHost = new Uri(info.Url).Host;
            var clientCertificates = new X509CertificateCollection(new X509Certificate[] { storeCertificate });

#if !NETSTANDARD
            await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = targetHost,
                ClientCertificates = clientCertificates,
                EnabledSslProtocols = SupportedSslProtocols,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                CipherSuitesPolicy = cipherSuitesPolicy
            }
#if !NETSTANDARD
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

        internal struct ConnectSecuredTcpSocketResult : IDisposable
        {
            public string Url;
            public TcpClient TcpClient;
            public Stream Stream;
            public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures;

            public ConnectSecuredTcpSocketResult(string url, TcpClient tcpClient, Stream stream, TcpConnectionHeaderMessage.SupportedFeatures supportedFeatures)
            {
                Url = url;
                TcpClient = tcpClient;
                Stream = stream;
                SupportedFeatures = supportedFeatures;
            }

            public void Dispose()
            {
                using (TcpClient)
                using (Stream)
                {
                }
            }
        }

        public delegate Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiationCallback(string url, TcpConnectionInfo tcpInfo, Stream stream, JsonOperationContext context, List<string> logs = null);

        internal static async Task<ConnectSecuredTcpSocketResult> ConnectSecuredTcpSocket(
            TcpConnectionInfo info,
            X509Certificate2 cert,
#if !NETSTANDARD
            CipherSuitesPolicy cipherSuitesPolicy,
#endif
            TcpConnectionHeaderMessage.OperationTypes operationType,
            NegotiationCallback negotiationCallback,
            JsonOperationContext negContext,
            TimeSpan? timeout,
            List<string> logs = null
#if !NETSTANDARD
            ,
            CancellationToken token = default
#endif
            )
        {
            var infoUrls = info.Urls == null
                ? new[] {info.Url}
                : info.Urls.Contains(info.Url)
                    ? info.Urls
                    : info.Urls.Append(info.Url).ToArray();

            logs?.Add($"Received tcpInfo: {Environment.NewLine}urls: { string.Join(", ", infoUrls)} {Environment.NewLine}guid:{ info.ServerId}");

            var exceptions = new List<Exception>();
            foreach (var url in infoUrls)
            {
                TcpClient tcpClient = null;
                Stream stream = null;
                try
                {
                    logs?.Add($"Trying to connect to :{url}");

                    tcpClient = await ConnectAsync(
                        url,
                        timeout
#if !NETSTANDARD
                        ,
                        token: token
#endif
                        ).ConfigureAwait(false);

                    stream = await WrapStreamWithSslAsync(
                        tcpClient,
                        info,
                        cert,
#if !NETSTANDARD
                        cipherSuitesPolicy,
#endif
                        timeout
#if !NETSTANDARD
                        ,
                        token: token
#endif
                    ).ConfigureAwait(false);

                    var supportedFeatures = await InvokeNegotiation(info, operationType, negotiationCallback, negContext, logs, url, stream).ConfigureAwait(false);

                    logs?.Add($"{Environment.NewLine}Negotiation successful for operation {operationType}.{Environment.NewLine} {tcpClient.Client.LocalEndPoint} "+
                              $"is connected to {tcpClient.Client.RemoteEndPoint}{Environment.NewLine}");

                    return new ConnectSecuredTcpSocketResult(url, tcpClient, stream, supportedFeatures);
                }
                catch (Exception e)
                {
                    tcpClient?.Dispose();
                    stream?.Dispose();

                    logs?.Add($"Failed to connect to url {url}: {e.Message}");
                    exceptions.Add(e);
                }
            }

            var message = $"Failed to connect to all urls {string.Join(", ", infoUrls)}";
            if (logs != null)
                message += Environment.NewLine + string.Join(Environment.NewLine, logs);

            throw new AggregateException(message, exceptions);
        }

        private static async Task<TcpConnectionHeaderMessage.SupportedFeatures> InvokeNegotiation(TcpConnectionInfo info, TcpConnectionHeaderMessage.OperationTypes operationType, NegotiationCallback negotiationCallback,
            JsonOperationContext negContext, List<string> logs, string url, Stream stream)
        {
            switch (operationType)
            {
                case TcpConnectionHeaderMessage.OperationTypes.Subscription:
                case TcpConnectionHeaderMessage.OperationTypes.Replication:
                case TcpConnectionHeaderMessage.OperationTypes.Heartbeats:
                case TcpConnectionHeaderMessage.OperationTypes.Cluster:
                    return await negotiationCallback(url, info, stream, negContext).ConfigureAwait(false);
                case TcpConnectionHeaderMessage.OperationTypes.TestConnection:
                case TcpConnectionHeaderMessage.OperationTypes.Ping:
                    return await negotiationCallback(url, info, stream, negContext, logs).ConfigureAwait(false);
                default:
                    throw new NotSupportedException($"Operation type '{operationType}' not supported.");
            }
        }
    }
}
