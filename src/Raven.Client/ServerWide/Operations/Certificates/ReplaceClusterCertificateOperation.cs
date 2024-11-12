using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    /// <summary>
    /// Allows to replace an existing cluster certificate with a new one.
    /// </summary>
    public sealed class ReplaceClusterCertificateOperation : IServerOperation
    {
        private readonly byte[] _certBytes;
        private readonly bool _replaceImmediately;

        /// <inheritdoc cref="ReplaceClusterCertificateOperation"/>
        /// <param name="certBytes">Raw bytes of new certificate.</param>
        /// <param name="replaceImmediately">Indicates whether certificate should be replaced immediately by the server.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="certBytes"/> is null.</exception>
        public ReplaceClusterCertificateOperation(byte[] certBytes, bool replaceImmediately)
        {
            _certBytes = certBytes ?? throw new ArgumentNullException(nameof(certBytes));
            _replaceImmediately = replaceImmediately;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ReplaceClusterCertificateCommand(conventions, _certBytes, _replaceImmediately);
        }

        private sealed class ReplaceClusterCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly byte[] _certBytes;
            private readonly bool _replaceImmediately;

            public ReplaceClusterCertificateCommand(DocumentConventions conventions, byte[] certBytes, bool replaceImmediately)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _certBytes = certBytes ?? throw new ArgumentNullException(nameof(certBytes));
                _replaceImmediately = replaceImmediately;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates/replace-cluster-cert?replaceImmediately={_replaceImmediately}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(CertificateDefinition.Certificate));
                            writer.WriteString(Convert.ToBase64String(_certBytes)); // keep the private key -> this is a server cert
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
