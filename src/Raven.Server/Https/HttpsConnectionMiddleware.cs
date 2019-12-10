using System;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

namespace Raven.Server.Https
{
    public class HttpsConnectionMiddleware
    {
        private readonly RavenServer _server;

        // See http://oid-info.com/get/1.3.6.1.5.5.7.3.1
        // Indicates that a certificate can be used as a SSL server certificate
        private const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";

        private X509Certificate2 _serverCertificate;

        public HttpsConnectionMiddleware(RavenServer server, KestrelServerOptions options)
        {
            _server = server;

            options.ConfigureHttpsDefaults(o =>
            {
                o.SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls11 | SslProtocols.Tls;
                o.CheckCertificateRevocation = true;
                o.ClientCertificateMode = ClientCertificateMode.AllowCertificate;
                o.ClientCertificateValidation = (certificate, chain, sslPolicyErrors) =>
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
                };

                o.OnAuthenticate = (context, authenticationOptions) =>
                {
                    authenticationOptions.ServerCertificate = _serverCertificate;
                };
            });
        }

        public void SetCertificate(X509Certificate2 serverCertificate)
        {
            EnsureCertificateIsAllowedForServerAuth(serverCertificate);

            Interlocked.Exchange(ref _serverCertificate, serverCertificate);
        }

        public async Task OnConnectionAsync(ConnectionContext context, Func<Task> next)
        {
            var tlsHandshakeFeature = context.Features.Get<ITlsHandshakeFeature>();
            if (tlsHandshakeFeature != null)
            {
                if (tlsHandshakeFeature.Protocol != SslProtocols.Tls12)
                {
                    context.Features.Set<IHttpAuthenticationFeature>(new RavenServer.AuthenticateConnection
                    {
                        WrongProtocolMessage =
                            $"RavenDB requires clients to connect using TLS 1.2, but the client used: '{tlsHandshakeFeature.Protocol}'."
                    });

                    await next();
                    return;
                }
            }

            if (_server.ServerStore.Initialized == false)
                await _server.ServerStore.InitializationCompleted.WaitAsync();

            var tlsConnectionFeature = context.Features.Get<ITlsConnectionFeature>();
            X509Certificate2 certificate = null;
            if (tlsConnectionFeature != null)
                certificate = await tlsConnectionFeature.GetClientCertificateAsync(context.ConnectionClosed);

            var httpConnectionFeature = context.Features.Get<IHttpConnectionFeature>();
            var authenticationStatus = _server.AuthenticateConnectionCertificate(certificate, httpConnectionFeature);

            // build the token
            context.Features.Set<IHttpAuthenticationFeature>(authenticationStatus);

            await next();
        }

        internal static X509Certificate2 ConvertToX509Certificate2(X509Certificate certificate)
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
    }
}
