// Initially from: https://github.com/aspnet/KestrelHttpServer/blob/a31d1e024ca86a4a9053e0be105fde17c5ccae10/src/Kestrel.Https/Internal/HttpsConnectionAdapter.cs
// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core.Adapter.Internal;
using Sparrow.Logging;

namespace Raven.Server.Https
{

    public class HttpsConnectionAdapter : IConnectionAdapter
    {
        // See http://oid-info.com/get/1.3.6.1.5.5.7.3.1
        // Indicates that a certificate can be used as a SSL server certificate
        private const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";

        private static readonly ClosedAdaptedConnection _closedAdaptedConnection = new ClosedAdaptedConnection();

        private X509Certificate2 _serverCertificate;
        private readonly Logger _logger;

        public HttpsConnectionAdapter(X509Certificate2 certificate) : this()
        {
            SetCertificate(certificate);
        }

        public HttpsConnectionAdapter()
        {
            _logger = LoggingSource.Instance.GetLogger<HttpsConnectionAdapter>("Server");
        }


        public void SetCertificate(X509Certificate2 serverCertificate)
        {
            EnsureCertificateIsAllowedForServerAuth(serverCertificate);

            Interlocked.Exchange(ref _serverCertificate, serverCertificate);
        }

        public bool IsHttps => true;

        public Task<IAdaptedConnection> OnConnectionAsync(ConnectionAdapterContext context)
        {
            // Don't trust SslStream not to block.
            return Task.Run(() => InnerOnConnectionAsync(context));
        }

        private async Task<IAdaptedConnection> InnerOnConnectionAsync(ConnectionAdapterContext context)
        {
            var sslStream = new SslStream(context.ConnectionStream,
                leaveInnerStreamOpen: false,
                userCertificateValidationCallback: (sender, certificate, chain, sslPolicyErrors) =>
                {
                    if (certificate == null)
                    {
                        return true; // we handle the error from not having certificate higher in the stack
                    }

                    var certificate2 = ConvertToX509Certificate2(certificate);
                    if (certificate2 == null)
                        return false; // we require to be able to convert it in all cases

                    // Here we are explicitly ignoring trust chain issues for client certificates
                    // this is because we don't actually require trust, we just use the certificate
                    // as a way to authenticate. The admin is going to tell us which specific certs
                    // we can trust anyway, so we can ignore such errors.

                    return sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors ||
                           sslPolicyErrors == SslPolicyErrors.None;
                });

            try
            {
                await sslStream.AuthenticateAsServerAsync(
                    _serverCertificate, 
                    clientCertificateRequired: true,
                    enabledSslProtocols: SslProtocols.Tls12 | SslProtocols.Tls11 | SslProtocols.Tls, 
                    checkCertificateRevocation: true);
            }
            catch (OperationCanceledException)
            {
                sslStream.Dispose();
                return _closedAdaptedConnection;
            }
            catch (IOException ex)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Failed to authenticate client", ex);
                sslStream.Dispose();
                return _closedAdaptedConnection;
            }

            // Always set the feature even though the cert might be null
            context.Features.Set<ITlsConnectionFeature>(new TlsConnectionFeature
            {
                ClientCertificate = ConvertToX509Certificate2(sslStream.RemoteCertificate)
            });

            return new HttpsAdaptedConnection(sslStream);
        }

        private static void EnsureCertificateIsAllowedForServerAuth(X509Certificate2 certificate)
        {
            /* If the Extended Key Usage extension is included, then we check that the serverAuth usage is included. (http://oid-info.com/get/1.3.6.1.5.5.7.3.1)
             * If the Extended Key Usage extension is not included, then we assume the certificate is allowed for all usages.
             *
             * See also https://blogs.msdn.microsoft.com/kaushal/2012/02/17/client-certificates-vs-server-certificates/
             *
             * From https://tools.ietf.org/html/rfc3280#section-4.2.1.13 "Certificate Extensions: Extended Key Usage"
             *
             * If the (Extended Key Usage) extension is present, then the certificate MUST only be used
             * for one of the purposes indicated.  If multiple purposes are
             * indicated the application need not recognize all purposes indicated,
             * as long as the intended purpose is present.  Certificate using
             * applications MAY require that a particular purpose be indicated in
             * order for the certificate to be acceptable to that application.
             */

            var hasEkuExtension = false;

            foreach (var extension in certificate.Extensions.OfType<X509EnhancedKeyUsageExtension>())
            {
                hasEkuExtension = true;
                foreach (var oid in extension.EnhancedKeyUsages)
                {
                    if (oid.Value.Equals(ServerAuthenticationOid, StringComparison.Ordinal))
                    {
                        return;
                    }
                }
            }

            if (hasEkuExtension)
            {
                throw new InvalidOperationException($"Certificate {certificate.Thumbprint} cannot be used as an SSL server certificate. It has an Extended Key Usage extension but the usages do not include Server Authentication (OID 1.3.6.1.5.5.7.3.1)");
            }
        }

        public static X509Certificate2 ConvertToX509Certificate2(X509Certificate certificate)
        {
            if (certificate == null)
            {
                return null;
            }

            if (certificate is X509Certificate2 cert2)
            {
                return cert2;
            }

            return new X509Certificate2(certificate);
        }

        public class HttpsAdaptedConnection : IAdaptedConnection
        {
            private readonly SslStream _sslStream;

            public HttpsAdaptedConnection(SslStream sslStream)
            {
                _sslStream = sslStream;
            }

            public SslProtocols SslProtocol => _sslStream.SslProtocol;


            public Stream ConnectionStream => _sslStream;

            public void Dispose()
            {
                _sslStream.Dispose();
            }
        }

        private class ClosedAdaptedConnection : IAdaptedConnection
        {
            public Stream ConnectionStream { get; } = Stream.Null;

            public void Dispose()
            {
            }
        }
    }
}
