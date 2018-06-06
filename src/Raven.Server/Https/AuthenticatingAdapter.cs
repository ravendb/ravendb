using System;
using System.IO;
using System.IO.Pipelines;
using System.Reflection;
using System.Reflection.Emit;
using System.Security.Authentication;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.Server.Kestrel.Core.Adapter.Internal;
using Sparrow.Logging;

namespace Raven.Server.Https
{
    public class AuthenticatingAdapter : IConnectionAdapter
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<AuthenticatingAdapter>("Server");

        private readonly RavenServer _server;
        private readonly HttpsConnectionAdapter _httpsConnectionAdapter;
        private static readonly Func<RawStream, PipeReader> GetInput;

        static AuthenticatingAdapter()
        {
            var field = typeof(RawStream).GetField("_input", BindingFlags.Instance | BindingFlags.NonPublic);
            var getter = new DynamicMethod("GetInput", typeof(PipeReader), new[]
            {
                typeof(RawStream),
            });
            var ilGenerator = getter.GetILGenerator();

            ilGenerator.Emit(OpCodes.Ldarg_0);
            ilGenerator.Emit(OpCodes.Ldfld, field);
            ilGenerator.Emit(OpCodes.Ret);
            GetInput = (Func<RawStream, PipeReader>)getter.CreateDelegate(
                typeof(Func<RawStream, PipeReader>));
        }

        public AuthenticatingAdapter(RavenServer server, HttpsConnectionAdapter httpsConnectionAdapter)
        {
            _server = server;
            _httpsConnectionAdapter = httpsConnectionAdapter;
        }

        private class SameConnectionStream : IAdaptedConnection
        {
            public SameConnectionStream(Stream connectionStream)
            {
                ConnectionStream = connectionStream;
            }

            public void Dispose()
            {
            }

            public Stream ConnectionStream { get; }
        }

        public async Task<IAdaptedConnection> OnConnectionAsync(ConnectionAdapterContext context)
        {
            if (context.ConnectionStream is RawStream rs)
            {
                // here we do protocol sniffing to see if user is trying to access us via
                // http while we are expecting HTTPS.

                var input = GetInput(rs); // uses a delegate to get the private RawStream._input out
                // here we take advantage of the fact that Kestrel allow to get the data from the buffer
                // without actually consuming it
                var result = await input.ReadAsync();
                try
                {
                    if (result.Buffer.First.IsEmpty == false)
                    {
                        var b = result.Buffer.First.Span[0];
                        if (b >= 'A' && b <= 'Z')
                        {
                            // this is a good indication that we have been connected using HTTP, instead of HTTPS
                            // because the first characeter is a valid ASCII value. However, in SSL2, the first bit
                            // is always on, and in SSL 3 / TLS 1.0 - 1.2 the first byte is 22.
                            // https://stackoverflow.com/questions/3897883/how-to-detect-an-incoming-ssl-https-handshake-ssl-wire-format
                            context.Features.Set<IHttpAuthenticationFeature>(new RavenServer.AuthenticateConnection
                            {
                                WrongProtocolMessage = "Attempted to access an HTTPS server using HTTP, did you forget to change 'http://' to 'https://' ?"
                            });
                            return new SameConnectionStream(rs);
                        }
                    }
                }
                finally
                {
                    input.AdvanceTo(result.Buffer.Start, result.Buffer.Start);
                }
            }

            var connection = await _httpsConnectionAdapter.OnConnectionAsync(context);
            if (connection is HttpsConnectionAdapter.HttpsAdaptedConnection c)
            {
                if (c.SslProtocol != SslProtocols.Tls12)
                {
                    context.Features.Set<IHttpAuthenticationFeature>(new RavenServer.AuthenticateConnection
                    {
                        WrongProtocolMessage = "RavenDB requires clients to connect using TLS 1.2, but the client used: '" + c.SslProtocol + "'."
                    });

                    return c;
                }
            }

            var tls = context.Features.Get<ITlsConnectionFeature>();
            var certificate = tls?.ClientCertificate;
            var authenticationStatus = _server.AuthenticateConnectionCertificate(certificate);

            // build the token
            context.Features.Set<IHttpAuthenticationFeature>(authenticationStatus);

            return connection;
        }

        public bool IsHttps => true;
    }
}
